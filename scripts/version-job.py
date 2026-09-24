#!/usr/bin/env python3
"""Render isolated Flink/query release artifacts. Never applies resources to a cluster."""
import argparse
import copy
import json
from pathlib import Path
import re
import shutil
import yaml

ROOT = Path(__file__).resolve().parents[1]


def read(path):
    with Path(path).open() as file:
        return yaml.safe_load(file)


def dump(path, value):
    Path(path).write_text(yaml.safe_dump(value, sort_keys=False))


def version_config(job, queries, version):
    if not re.fullmatch(r'v[0-9][a-z0-9-]{0,20}', version):
        raise ValueError('Version must match v[0-9][a-z0-9-]{0,20}, for example v2 or v2-canary')
    job, queries = copy.deepcopy(job), copy.deepcopy(queries)
    if not job['namespace'].startswith('context_secure') or not job['name'].startswith('context-secure-'):
        raise ValueError('Only labeled secure jobs can be versioned; legacy state requires fresh ingestion')
    name = 'context-secure-' + version
    if name in (job['name'], job['consumerGroup']):
        raise ValueError('Candidate version must differ from the input job')
    if not re.fullmatch(r'[A-Za-z_][A-Za-z0-9_]*', job['namespace']):
        raise ValueError('Iceberg namespace must be a simple identifier')
    output_tables = [value['table'] for value in job['outputs'].values()]
    output_topics = [value['topic'] for value in job['outputs'].values() if value.get('topic')] + [job['errorTopic']]
    if len(set(output_tables)) != len(output_tables) or len(set(output_topics)) != len(output_topics):
        raise ValueError('Input job output tables and topics must be distinct')
    for table in output_tables:
        if not re.fullmatch(r'[A-Za-z_][A-Za-z0-9_]*', table):
            raise ValueError('Table must be a simple identifier: ' + table)
    for topic in output_topics + job['sourceTopics']:
        if not topic.startswith('cg.secure.') or not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9._-]{0,219}', topic):
            raise ValueError('Invalid or too-long Kafka topic: ' + topic)
    namespace = job['namespace'] + '_' + version.replace('-', '_')
    table_map, topic_map = {}, {}
    for output in job['outputs'].values():
        table_map[job['namespace'] + '.' + output['table']] = namespace + '.' + output['table']
        if output.get('topic'):
            topic_map[output['topic']] = output['topic'] + '.' + version
            output['topic'] = topic_map[output['topic']]
    topic_map[job['errorTopic']] = job['errorTopic'] + '.' + version
    job['errorTopic'] = topic_map[job['errorTopic']]
    if set(topic_map.values()) & set(job['sourceTopics']):
        raise ValueError('Candidate output topics collide with source topics')
    if len(set(topic_map.values())) != len(topic_map):
        raise ValueError('Output topic collision')
    job.update(name=name, consumerGroup=name, namespace=namespace)
    if 'registeredTables' in queries:
        queries['registeredTables'] = {table_map.get(table, table): policy for table, policy in queries['registeredTables'].items()}
    for query in queries['queries'].values():
        if query['table'] not in table_map:
            raise ValueError('Query references an unmapped table: ' + query['table'])
        query['table'] = table_map[query['table']]
    for subscription in queries['subscriptions'].values():
        subscription['topics'] = [topic_map.get(topic, topic) for topic in subscription['topics']]
    return job, queries, table_map, topic_map


def config_volume(manifest, name, items):
    template = manifest['spec'].get('podTemplate', manifest['spec'].get('template'))
    if not template:
        raise ValueError('Base manifest requires a pod template')
    found = False
    for volume in template['spec'].get('volumes', []):
        if 'configMap' in volume and volume['configMap'].get('name', '').startswith('context-config'):
            volume['configMap'] = dict(name=name, items=items)
            found = True
    if not found:
        raise ValueError('Base manifest requires a configMap volume')
    return template


def render(args):
    job, queries, table_map, topic_map = version_config(read(args.job_config), read(args.query_config), args.version)
    output = Path(args.output)
    if output.exists() and any(output.iterdir()):
        raise ValueError('Output directory must be empty; choose a new version directory')
    flink = next(d for d in yaml.safe_load_all(Path(args.flink_manifest).read_text()) if d and d.get('kind') == 'FlinkDeployment')
    docs = list(yaml.safe_load_all(Path(args.apps_manifest).read_text()))
    query = copy.deepcopy(next(d for d in docs if d and d.get('kind') == 'Deployment' and d['metadata']['name'] == 'query'))
    topic_job = read(args.topics_manifest)
    output.mkdir(parents=True, exist_ok=True)
    config = output / 'config'
    config.mkdir()
    source_config = Path(args.job_config).resolve().parent
    for path in source_config.rglob('*'):
        if path.is_file() and path.suffix in ('.yaml', '.yml', '.json'):
            destination = config / path.relative_to(source_config)
            destination.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(path, destination)
    dump(config / 'jobs.yaml', job)
    dump(config / 'queries.yaml', queries)
    query_runtime = read(config / 'query.yaml')
    query_runtime['queries'] = '/app/config/queries.yaml'
    dump(config / 'query.yaml', query_runtime)
    data, items = {}, []
    for path in sorted(config.rglob('*')):
        if path.is_file():
            relative = path.relative_to(config).as_posix()
            key = relative.replace('/', '__')
            if key in data:
                raise ValueError('ConfigMap filename collision: ' + relative)
            data[key] = path.read_text()
            items.append(dict(key=key, path=relative))
    name = job['name']
    namespace = flink['metadata'].get('namespace', 'context-graph')
    if not re.fullmatch(r'[a-z0-9]([-a-z0-9]*[a-z0-9])?', namespace):
        raise ValueError('Invalid Kubernetes namespace')
    config_name = name + '-config'
    dump(output / 'configmap.yaml', dict(apiVersion='v1', kind='ConfigMap', metadata=dict(name=config_name, namespace=namespace), immutable=True, data=data))
    flink['metadata']['name'] = name
    flink['spec']['job']['state'] = 'running'
    flink['spec']['job']['parallelism'] = job['parallelism']
    flink['spec']['job'].pop('initialSavepointPath', None)
    flink['spec']['job'].pop('savepointRedeployNonce', None)
    flink['spec']['job']['args'] = ['/app/config/jobs.yaml']
    # Separate operator HA, checkpoint, and savepoint directories as well as sink transaction prefixes.
    fc = flink['spec'].setdefault('flinkConfiguration', {})
    for key in ('high-availability.storageDir', 'execution.checkpointing.dir', 'execution.checkpointing.savepoint-dir', 'state.checkpoints.dir', 'state.savepoints.dir'):
        if key in fc:
            fc[key] = fc[key].rstrip('/') + '/' + name
    template = config_volume(flink, config_name, items)
    for container in template['spec']['containers']:
        env = container.setdefault('env', [])
        env[:] = [e for e in env if e['name'] != 'JOB_ID']
        env.append(dict(name='JOB_ID', value=name))
        for entry in env:
            if entry['name'] in ('CHECKPOINT_URI', 'SAVEPOINT_URI') and 'value' in entry:
                entry['value'] = entry['value'].rstrip('/') + '/' + name
    if args.node:
        template['spec']['nodeSelector'] = {'kubernetes.io/hostname': args.node}
    dump(output / 'flink.yaml', flink)
    candidate = 'query-' + args.version
    labels = dict(app=candidate, release=args.version, **{'context-graph-role':'query'})
    query['metadata'].update(name=candidate, namespace=namespace)
    query['spec']['selector']['matchLabels'] = labels.copy()
    query['spec']['template']['metadata']['labels'] = labels.copy()
    config_volume(query, config_name, items)
    if args.node:
        query['spec']['template']['spec']['nodeSelector'] = {'kubernetes.io/hostname': args.node}
    service = dict(apiVersion='v1', kind='Service', metadata=dict(name=candidate, namespace=namespace), spec=dict(selector=labels.copy(), ports=[dict(name='http', port=8081)]))
    pdb = dict(apiVersion='policy/v1',kind='PodDisruptionBudget',metadata=dict(name=candidate,namespace=namespace),spec=dict(minAvailable=1,selector=dict(matchLabels=labels.copy())))
    (output / 'query.yaml').write_text(yaml.safe_dump_all([query, service, pdb], sort_keys=False))
    topic_job['metadata'].update(name=name + '-topics', namespace=namespace)
    container = topic_job['spec']['template']['spec']['containers'][0]
    command = 'set -euo pipefail\nK=/opt/kafka/bin\n'
    command += 'acl() { "$K/kafka-acls.sh" --bootstrap-server secure-kafka:9092 --command-config /app/secrets/kafka.properties --add "$@"; }\n'
    for topic in sorted(topic_map.values()):
        command += f'"$K/kafka-topics.sh" --bootstrap-server secure-kafka:9092 --command-config /app/secrets/kafka.properties --create --if-not-exists --topic {topic} --partitions 6 --replication-factor 3 --config min.insync.replicas=2\n'
        command += f'acl --allow-principal User:processor --operation Write --operation Describe --topic {topic}\n'
        if topic != job['errorTopic']:
            command += f'acl --allow-principal User:query --operation Read --operation Describe --topic {topic}\n'
    command += f'acl --allow-principal User:processor --operation Read --group {name}\n'
    command += f'acl --allow-principal User:processor --operation Write --operation Describe --transactional-id {name} --resource-pattern-type prefixed\n'
    container['args'][0] = command
    if args.node:
        topic_job['spec']['template']['spec']['nodeSelector'] = {'kubernetes.io/hostname': args.node}
    dump(output / 'topics.yaml', topic_job)
    (output / 'release.json').write_text(json.dumps(dict(version=args.version, jobId=name, consumerGroup=job['consumerGroup'], sourceTopics=job['sourceTopics'], tables=table_map, topics=topic_map, queryService=candidate, namespace=namespace), indent=2) + '\n')
    patch = json.dumps([dict(op='replace',path='/spec/selector',value=labels)], separators=(',', ':'))
    # Cutover is an explicit second command, gated on rollout readiness and recorded human warmup review.
    (output / 'cutover.sh').write_text(f'''#!/bin/sh
set -eu
if [ "${{WARMUP_VERIFIED:-}}" != "{args.version}" ]; then
  echo "Compare candidate committed windows and source lag, then set WARMUP_VERIFIED={args.version}." >&2
  exit 1
fi
kubectl --context "${{KUBE_CONTEXT:?Set KUBE_CONTEXT explicitly}}" -n {namespace} rollout status deployment/{candidate} --timeout=180s
if [ ! -f "$(dirname "$0")/rollback-service.json" ]; then
  kubectl --context "${{KUBE_CONTEXT:?Set KUBE_CONTEXT explicitly}}" -n {namespace} get service query -o json > "$(dirname "$0")/rollback-service.json"
fi
kubectl --context "${{KUBE_CONTEXT:?Set KUBE_CONTEXT explicitly}}" -n {namespace} patch service query --type=json -p '{patch}'
echo "New connections route to {candidate}. Existing sockets stay on the previous release until reconnect."
''')
    (output / 'cutover.sh').chmod(0o755)
    (output / 'rollback.sh').write_text('''#!/bin/sh
set -eu
python3 - "$(dirname "$0")/rollback-service.json" <<'PY'
import json, os, subprocess, sys
context=os.environ['KUBE_CONTEXT']
service=json.load(open(sys.argv[1]))
patch=json.dumps([dict(op='replace',path='/spec/selector',value=service['spec']['selector'])])
subprocess.run(['kubectl','--context',context,'-n',service['metadata']['namespace'],'patch','service',service['metadata']['name'],'--type=json','-p',patch],check=True)
PY
''')
    (output / 'rollback.sh').chmod(0o755)
    print('Rendered ' + str(output) + '; no cluster changes made.')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--version', required=True)
    parser.add_argument('--job-config', default=str(ROOT / 'config/jobs.yaml'))
    parser.add_argument('--query-config', default=str(ROOT / 'config/queries.yaml'))
    parser.add_argument('--flink-manifest', default=str(ROOT / 'deploy/k8s/flink.yaml'))
    parser.add_argument('--apps-manifest', default=str(ROOT / 'deploy/k8s/apps.yaml'))
    parser.add_argument('--topics-manifest', default=str(ROOT / 'deploy/k8s/secure-topics.yaml'))
    parser.add_argument('--output', required=True)
    parser.add_argument('--node', help='Pin candidate pods to the same node as the local data volume')
    try:
        render(parser.parse_args())
    except (ValueError, KeyError, FileNotFoundError) as error:
        parser.exit(2, str(error) + '\n')


if __name__ == '__main__':
    main()
