"""Compile a restricted application bundle. No credentials or cluster writes occur here."""
from __future__ import annotations
import copy, hashlib, json, os, re, shutil, subprocess
from pathlib import Path
import yaml, jsonschema, sqlglot
from sqlglot import exp
ROOT = Path(__file__).resolve().parents[3]
SCOPE = {'workspace_id', 'resource_id', 'entity_id'}
FUNCTIONS = {'JSON_VALUE','JSON_QUERY','JSON_STRING','PARSE_JSON','TRY_PARSE_JSON','TUMBLE','TUMBLE_START','TUMBLE_END','HOP','HOP_START','HOP_END','COUNT','SUM','AVG','MIN','MAX','CAST','TRY_CAST','COALESCE','NULLIF','LOWER','UPPER','LENGTH','CONCAT','SUBSTRING','ROUND','ABS','FLOOR','CEIL','IF','CASE','IFNULL','GREATEST','LEAST'}
class Invalid(ValueError): pass

def identifier(value, pattern=r'[a-z][a-z0-9_]{0,39}'):
    if not isinstance(value,str) or not re.fullmatch(pattern,value):raise Invalid('Invalid identifier: '+str(value))
    return value

def keys(obj, allowed, required=()):
    if not isinstance(obj,dict) or set(obj)-set(allowed) or set(required)-set(obj):raise Invalid('Invalid fields; allowed='+','.join(sorted(allowed)))

def read_file(root, relative):
    if not isinstance(relative,str):raise Invalid('File path required')
    path=(root/relative).resolve()
    if not path.is_relative_to(root.resolve()) or not path.is_file() or path.stat().st_size>256_000:raise Invalid('Invalid bundle file: '+relative)
    return path.read_text()

def sql_policy(sql, sources):
    """Conservative resource-local relational subset; engine planning is a second gate."""
    if len(sql)>32_000:raise Invalid('SQL too large')
    try: trees=sqlglot.parse(sql)
    except sqlglot.errors.ParseError as e:raise Invalid('SQL parse error') from e
    if len(trees)!=1 or not isinstance(trees[0],exp.Select):raise Invalid('One SELECT per view is required')
    tree=trees[0]
    if tree.args.get('with') or tree.args.get('joins') or tree.args.get('limit') or tree.args.get('offset') or tree.args.get('order') or tree.args.get('into'):
        raise Invalid('Flink v1 views require resource-local SELECTs without joins, CTEs, limits, or global ordering')
    if len(list(tree.find_all(exp.Select)))!=1 or tree.find(exp.Subquery) or tree.find(exp.Window):raise Invalid('Subqueries and analytic windows need an explicit future scope rule')
    tables=list(tree.find_all(exp.Table))
    if len(tables)!=1 or tables[0].name not in sources or tables[0].db or tables[0].catalog:raise Invalid('View must read one declared logical source')
    if not isinstance(tree.args.get('from').this,exp.Table):raise Invalid('Table functions are not admitted')
    projections={}
    for item in tree.expressions:
        name=item.alias_or_name
        identifier(name,r'[A-Za-z_][A-Za-z0-9_]{0,63}')
        if name in projections:raise Invalid('Duplicate output alias')
        projections[name]=item.this if isinstance(item,exp.Alias) else item
    for name in SCOPE:
        expression=projections.get(name)
        if not isinstance(expression,exp.Column) or expression.name!=name:raise Invalid('Scope columns must pass through unchanged: '+name)
    group=tree.args.get('group')
    if group:
        if any(group.args.get(k) for k in ('grouping_sets','cube','rollup','all')):raise Invalid('Grouping sets can discard scope')
        grouping={e.name for e in group.expressions if isinstance(e,exp.Column)}
        if not SCOPE<=grouping:raise Invalid('GROUP BY must preserve workspace/resource/entity scope')
    elif tree.find(exp.AggFunc):raise Invalid('Aggregates require resource-scoped GROUP BY')
    alias=tables[0].alias_or_name
    for col in tree.find_all(exp.Column):
        if col.table and col.table!=alias:raise Invalid('Undeclared column qualifier')
    for fun in tree.find_all(exp.Func):
        name=fun.name.upper() if isinstance(fun,exp.Anonymous) else fun.sql_name()
        if name not in FUNCTIONS:raise Invalid('Unsupported function: '+name)
    return tree

def java_tool(module, main, args):
    target=ROOT/'services'/module/'target'
    cp=target/'harness-classpath.txt'
    supplied=os.environ.get('CG_'+module.upper()+'_CLASSPATH')
    if not supplied and not cp.exists():
        subprocess.run(['mvn','-q','-f',str(ROOT/'services'/module/'pom.xml'),'dependency:build-classpath','-Dmdep.outputFile='+str(cp)],check=True)
    java=Path(os.environ['JAVA_HOME'])/'bin/java' if os.environ.get('JAVA_HOME') else 'java'
    subprocess.run([str(java),'--add-opens=java.base/java.lang=ALL-UNNAMED','--add-opens=java.base/java.util=ALL-UNNAMED',
        '-Dorg.slf4j.simpleLogger.defaultLogLevel=error','-cp',(supplied or str(target/'classes')+os.pathsep+cp.read_text().strip()),main,*map(str,args)],check=True)

def compile_bundle(path, output):
    path=Path(path).resolve();root=path.parent;bundle=yaml.safe_load(read_file(root,path.name))
    keys(bundle,{'version','name','workspace','inputs','jobs','api','description'}, {'version','name','workspace','inputs','jobs','api'})
    if bundle['version']!=1:raise Invalid('Unsupported bundle version')
    name=identifier(bundle['name'],r'[a-z][a-z0-9-]{0,23}');workspace=identifier(bundle['workspace'],r'[A-Za-z0-9_-]{1,64}')
    files={path.name:path.read_text()}
    def read(rel):files[rel]=read_file(root,rel);return files[rel]
    inputs={}
    if not isinstance(bundle['inputs'],dict) or not 1<=len(bundle['inputs'])<=12:raise Invalid('One to twelve inputs required')
    for alias,item in bundle['inputs'].items():
        identifier(alias);keys(item,{'schema'},{'schema'});schema=json.loads(read(item['schema']));jsonschema.Draft202012Validator.check_schema(schema)
        inputs[alias]=schema
    jobs=[];available=set(inputs);view_owners={}
    if not isinstance(bundle['jobs'],dict) or not 1<=len(bundle['jobs'])<=8:raise Invalid('One to eight jobs required')
    for job_name,job in bundle['jobs'].items():
        identifier(job_name,r'[a-z][a-z0-9_]{0,15}');keys(job,{'sources','views','parallelism'},{'sources','views'})
        if not isinstance(job['sources'],list) or not job['sources'] or not set(job['sources'])<=set(inputs):raise Invalid('Job sources must name declared envelope inputs')
        local=set(job['sources']);views=[]
        for view_name,v in job['views'].items():
            identifier(view_name);keys(v,{'sql'},{'sql'})
            if view_name in available:raise Invalid('Duplicate view/source: '+view_name)
            sql=read(v['sql']);sql_policy(sql,local)
            views.append({'name':view_name,'sql':sql});local.add(view_name);available.add(view_name);view_owners[view_name]=job_name
        parallelism=job.get('parallelism',1)
        if not isinstance(parallelism,int) or not 1<=parallelism<=8:raise Invalid('Invalid job parallelism')
        jobs.append({'alias':job_name,'sources':job['sources'],'views':views,'parallelism':parallelism})
    api=bundle['api'];keys(api,{'queries','mutations','subscriptions'})
    for q in api.get('queries',{}).values():read(q['sql'])
    # Hash all authored material. Secrets never participate. Same bundle is reproducible.
    digest=hashlib.sha256(json.dumps(files,sort_keys=True).encode()).hexdigest()[:12]
    release='cg-'+name+'-'+digest
    namespace='context_secure_'+name.replace('-','_')+'_'+digest
    out=Path(output)
    if out.exists() and any(out.iterdir()):raise Invalid('Choose an empty output directory')
    out.mkdir(parents=True,exist_ok=True);(out/'schemas').mkdir()
    for f in ('envelope.json','error.json'):shutil.copyfile(ROOT/'config/schemas'/f,out/'schemas'/f)
    topics={alias:f'cg.secure.{name}.{digest}.in.{alias}' for alias in inputs}
    error_topic=f'cg.secure.{name}.{digest}.errors'
    ing=json.loads((ROOT/'config/ingestion.json').read_text());ing['endpoints']=[];ing['errorTopic']=error_topic
    for alias,schema in inputs.items():
        (out/'schemas'/f'{alias}.json').write_text(json.dumps(schema,indent=2))
        ing['endpoints'].append({'path':'/ingest/'+alias,'name':alias,'kind':'json','topic':topics[alias],'schema':f'schemas/{alias}.json','resourcePointer':'','eventTimePointer':''})
    (out/'ingestion.json').write_text(json.dumps(ing,indent=2))
    descriptions={};compiled_jobs=[];owners=set()
    for job in jobs:
        if owners.intersection(job['sources']):raise Invalid('Each persisted input has one owning job')
        owners.update(job['sources'])
        config={'name':release+'-'+job['alias'],'workspace':workspace,'namespace':namespace,'parallelism':job['parallelism'],
          'sources':[{'name':a,'topic':topics[a],'schema':f'schemas/{a}.json','table':a,'changeTopic':f'cg.secure.{name}.{digest}.out.{a}'} for a in job['sources']], 'views':[], 'errorTopic':error_topic}
        for view in job['views']:
            config['views'].append({**view,'table':view['name'],'topic':f'cg.secure.{name}.{digest}.out.'+view['name']})
        config_path=out/(job['alias']+'.json');config_path.write_text(json.dumps(config,indent=2))
        desc_path=out/(job['alias']+'.schemas.json')
        java_tool('processor','io.contextgraph.processor.SqlBundleJob',['--validate',config_path,desc_path])
        descriptions.update(json.loads(desc_path.read_text()));compiled_jobs.append(config)
    # The query compiler prepares SQL against empty typed relations. It never reads user data.
    query_input={'api':copy.deepcopy(api),'views':descriptions,'namespace':namespace,'inputs':inputs,'topics':{**{a:f'cg.secure.{name}.{digest}.out.{a}' for a in inputs},**{v['name']:v['topic'] for j in compiled_jobs for v in j['views']}}}
    for q in query_input['api'].get('queries',{}).values():q['sql']=files[q['sql']]
    (out/'api-input.json').write_text(json.dumps(query_input))
    java_tool('query','io.contextgraph.query.ApiCompiler',[out/'api-input.json',out/'queries.yaml'])
    runtime=yaml.safe_load((ROOT/'config/query.yaml').read_text());(out/'query.yaml').write_text(yaml.safe_dump(runtime))
    manifest={'version':1,'name':name,'release':release,'digest':digest,'workspace':workspace,'namespace':namespace,'jobs':compiled_jobs,
      'entrypoint':path.name,'inputs':topics,'errorTopic':error_topic,'views':descriptions,'sourceFiles':files}
    (out/'bundle.json').write_text(json.dumps(manifest,indent=2))
    return manifest
