#!/usr/bin/env python3
"""Build self-contained, editable SVG diagrams from locally vendored project marks."""
from pathlib import Path
from html import escape
import base64
import subprocess

ROOT = Path(__file__).resolve().parent
INK, MUTED, TEAL, BLUE, PURPLE = '#122d3a', '#526773', '#087e83', '#245ebe', '#7251a6'
class Diagram:
    def __init__(self, name, title, subtitle, height=1080):
        self.name=name; self.h=height
        self.p=[f'<svg xmlns="http://www.w3.org/2000/svg" xmlns:xlink="http://www.w3.org/1999/xlink" width="1440" height="{height}" viewBox="0 0 1440 {height}" role="img" aria-labelledby="title desc">',f'<title id="title">{escape(title)}</title><desc id="desc">{escape(subtitle)}</desc>', '<defs><marker id="arrow" markerWidth="8" markerHeight="8" refX="7" refY="4" orient="auto-start-reverse"><path d="M0 0L8 4L0 8" fill="none" stroke="#728995" stroke-width="1.5"/></marker></defs>',f'<rect width="1440" height="{height}" fill="#fff"/>','<g font-family="Arial, Helvetica, sans-serif">']
        self.text(56,44,'CONTEXT GRAPH  /  PLATFORM FIELD GUIDE',13,TEAL,700)
        self.text(56,98,title,36,INK,700)
        self.text(56,134,subtitle,18,MUTED)
    def text(self,x,y,s,size=18,color=INK,weight=400):
        self.p.append(f'<text x="{x}" y="{y}" font-size="{size}" font-weight="{weight}" fill="{color}">{escape(s)}</text>')
    def lines(self,x,y,ss,size=18,color=MUTED,step=27):
        for i,s in enumerate(ss):self.text(x,y+i*step,s,size,color)
    def rect(self,x,y,w,h,fill='#f2f6f7',radius=12):
        self.p.append(f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="{radius}" fill="{fill}"/>')
    def line(self,points,color='#728995',dash=False,arrow=True):
        pts=' '.join(f'{x},{y}' for x,y in points)
        self.p.append(f'<polyline points="{pts}" fill="none" stroke="{color}" stroke-width="2" stroke-linejoin="round"'+(' stroke-dasharray="6 6"' if dash else '')+(' marker-end="url(#arrow)"' if arrow else '')+'/>')
    def logo(self,name,x,y,size=44):
        p=ROOT/'logos'/f'{name}.svg'
        raw=p.read_bytes()
        if name=='iceberg': raw=raw.replace(b'<svg xmlns=', b'<svg viewBox="0 0 800 218" xmlns=',1)
        data=base64.b64encode(raw).decode()
        self.p.append(f'<image x="{x}" y="{y}" width="{130 if name == 'iceberg' else size}" height="{size}" xlink:href="data:image/svg+xml;base64,{data}"/>')
    def service(self,x,y,name,logo=None,size=25):
        if logo:self.logo(logo,x,y-30,38)
        if name=='Iceberg':return
        self.text(x+(52 if logo else 0),y,name,size,INK,700)
    def tag(self,x,y,n,label):
        self.text(x,y,n,14,TEAL,700);self.text(x+30,y,label,14,MUTED,700)
    def footer(self,s):
        self.line([(56,self.h-62),(1384,self.h-62)],'#d7e1e5',arrow=False)
        self.text(56,self.h-30,s,14,MUTED)
    def save(self):
        self.p.append('</g></svg>');svg='\n'.join(self.p)
        (ROOT/(self.name+'.svg')).write_text(svg)
        subprocess.run(['rsvg-convert','-w','1440','-o',str(ROOT/(self.name+'.png')),str(ROOT/(self.name+'.svg'))],check=True)
        return svg

d=Diagram('context-graph-overview','From events to shared, permissioned context','One platform; durable queries, live updates, media delivery, and delegated task execution.',1150)
for x,n,s in [(56,'01','INGEST'),(390,'02','PROCESS'),(724,'03','PERSIST'),(1058,'04','SERVE')]:d.tag(x,195,n,s)
# A continuous flow, with contracts underneath each stage.
for x in [56,390,724,1058]:d.rect(x,215,308,276,'#f4f7f8')
d.service(74,262,'Vert.x','vertx');d.service(408,262,'Kafka → Flink','flink');d.service(742,262,'Iceberg','iceberg');d.service(1076,262,'DuckDB','duckdb')
for x in [364,698,1032]:d.line([(x-8,317),(x+21,317)])
d.lines(74,312,['JSON · images · live video','Schema-validated endpoints','Trusted resource labels'],17)
d.lines(74,408,['Adapters / Temporal → endpoints','Per-endpoint topics + error queue'],16,TEAL)
d.lines(408,312,['Parallel SQL / stream jobs','Schema rows + SQL views','Event-time windows'],17)
d.lines(408,408,['Native Kubernetes deployments','Recovery state → private S3'],16,TEAL)
d.logo('polaris',742,293,22);d.text(772,312,'Polaris REST catalog',17,MUTED);d.lines(742,339,['RustFS private S3 warehouse','Snapshots + schema metadata'],17)
d.lines(742,408,['Service credential vending','Typed columns + VARIANT'],16,TEAL)
d.lines(1076,312,['iceberg + cache_httpfs','Authorized inputs → query SQL','Vert.x GraphQL API'],17)
d.lines(1076,408,['SQL views → derived fields','Recheck before returning data'],16,TEAL)
# Secondary paths are explicit routes, not confusing cross-page wires.
d.rect(56,515,1328,92,'#eef4ff')
d.logo('kafka',76,535,42);d.text(138,546,'LIVE',13,BLUE,700);d.text(138,576,'Kafka input + Flink output topics',19,INK,700)
d.line([(520,564),(620,564)]);d.lines(646,548,['GraphQL subscriptions','graphql-transport-ws · Reactor Flux'],17)
d.line([(1002,564),(1070,564)]);d.lines(1092,548,['Application clients','Authorize each emission'],17)
d.rect(56,627,1328,92,'#f2f7f3')
d.text(76,659,'MEDIA',13,TEAL,700);d.text(76,688,'FFmpeg keyframe chunks',19,INK,700)
d.line([(363,675),(465,675)]);d.lines(488,659,['RustFS media bucket','Bytes stay outside Kafka'],17)
d.line([(781,675),(882,675)]);d.lines(906,659,['Protected playlists, segments and ranges','Metadata joins the event / graph pipeline'],17)
d.rect(56,739,1328,132,'#f6f2fa')
d.text(76,772,'SHARED EXECUTION',13,PURPLE,700);d.service(76,808,'AX + Substrate','substrate',23)
d.lines(429,775,['Applications delegate tasks','Workers use scoped platform APIs','Run events use ordinary ingestion'],17)
d.lines(937,775,['Builders compile application bundles','Schemas + Flink SQL + query SQL','Release provisions platform resources'],17)
d.text(56,914,'CROSS-CUTTING SERVICES',13,TEAL,700)
d.lines(56,947,['OIDC + SpiceDB','User identity, workspace and entity access','Separate authorization graph'],16)
d.logo('kubernetes',512,922,31);d.lines(554,947,['Kubernetes + control plane','Read-only inventory, Flink status','Prometheus metrics and telemetry'],16)
d.logo('postgresql',1008,922,31);d.lines(1050,947,['PostgreSQL','SpiceDB / catalog / connector state','Isolated databases and roles'],16)
d.text(56,1054,'BUILD ON TOP',13,TEAL,700);d.text(223,1054,'Domain APIs     ·     Operational dashboards     ·     Search and chat     ·     Task-driven applications',19,INK,700)
d.footer('Storage credentials stay with platform services. Applications access the graph through permissioned APIs.')
svg=d.save()
# Keep existing deep links current, with no stale operator-era exports.
(ROOT/'context-graph-architecture.svg').write_text(svg)
subprocess.run(['rsvg-convert','-f','pdf','-o',str(ROOT/'context-graph-architecture.pdf'),str(ROOT/'context-graph-architecture.svg')],check=True)
(ROOT/'context-graph-architecture.png').write_bytes((ROOT/'context-graph-overview.png').read_bytes())
(ROOT/'context-graph-architecture.html').write_text('<!doctype html><html lang="en"><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>Context Graph architecture</title><style>body{margin:0;background:#eef2f4;font:16px sans-serif}nav{padding:20px}main{padding:20px}svg{width:100%;min-width:1000px;height:auto}a{color:#087e83}</style><nav>Context Graph · <a href="context-graph-overview.svg">Open editable SVG</a> · <a href="ax-task-execution.svg">AX execution</a> · <a href="permission-boundaries.svg">Permissions</a></nav><main>'+svg+'</main></html>')

d=Diagram('ax-task-execution','Tasks use the platform. Builders extend it.','AX coordinates workspaces and tasks; Substrate runs isolated workers. Application code defines the behavior.',1030)
d.tag(56,199,'01','APPLICATION TASKS');d.tag(774,199,'02','APPLICATION BUILDERS')
d.rect(56,221,610,115);d.rect(774,221,610,115)
d.text(80,258,'Ask, extract, correlate, or act',25,INK,700)
d.lines(80,290,['An application supplies the task and delegated access.','Chat is one shipped example of this contract.'],17)
d.text(798,258,'Define a new context application',25,INK,700)
d.lines(798,290,['Schemas + processing SQL + query SQL + access rules.','The cg harness compiles reviewable release artifacts.'],17)
d.line([(361,336),(361,377)]);d.line([(1079,336),(1079,377)])
d.rect(56,389,1328,163,'#f6f2fa');d.service(80,440,'AX',None,30);d.lines(80,478,['Task definitions','Workspaces + lifecycle'],18)
d.line([(332,467),(437,467)]);d.service(466,440,'Substrate','substrate',28);d.lines(466,478,['Isolated sandbox workers','Application image or builder image'],18)
d.line([(852,467),(948,467)]);d.text(978,440,'Scoped capabilities',25,INK,700);d.lines(978,478,['Delegated user / workspace context','Provider and deployment secrets','stay in trusted services'],17)
d.line([(361,552),(361,598)]);d.line([(1079,552),(1079,598)])
d.rect(56,612,610,201,'#eef6f6');d.rect(774,612,610,201,'#eef4ff')
d.text(80,651,'Use the existing graph',25,INK,700)
d.lines(80,689,['Query • ingest • subscribe through Context Graph APIs','SpiceDB checks resource access at the serving boundary.','Run events can use the default trajectory endpoint.'],17)
d.text(80,785,'RESULT → Application-owned output and user experience',16,TEAL,700)
d.text(798,651,'Release a new application bundle',25,INK,700)
d.lines(798,689,['Review → provision → deploy using the release workflow','Ingestion routes • Kafka topics / ACLs • Flink jobs','Iceberg tables / catalog grants • GraphQL fields'],17)
d.text(798,785,'RESULT → New data contracts and application views',16,BLUE,700)
d.text(56,865,'SHIPPED CHAT PATH',13,PURPLE,700)
d.lines(56,901,['User → coordinator / DeepSeek planning → AX retrieval task → permissioned query API','Coordinator rechecks evidence → DeepSeek synthesis → streamed, validated sections and citations'],19)
d.footer('Execution is shared infrastructure. Domain behavior belongs to each application; privileged release credentials stay outside task workers.')
d.save()

d=Diagram('permission-boundaries','Access follows the caller, all the way to results','JSON Schema defines application data. SpiceDB determines who can access each resource.',1120)
d.rect(56,180,1328,110,'#f6f2fa')
d.text(80,220,'OIDC identity',25,INK,700);d.lines(80,253,['Verified subject + workspace'],17)
d.line([(382,234),(471,234)],dash=True)
d.text(494,220,'SpiceDB authorization graph',25,INK,700);d.lines(494,253,['Workspace grants • restricted entities • imported source access'],17)
d.text(1140,220,'Check proxy',22,INK,700);d.text(1140,253,'Fully consistent checks',16,MUTED)
d.tag(56,337,'01','QUERY BOUNDARY')
d.rect(56,361,1328,240,'#f2f7f8')
for x,name,logo in [(80,'Private warehouse','polaris'),(525,'Authorized inputs','duckdb'),(1004,'Application result','vertx')]:d.service(x,413,name,logo,23)
d.lines(80,457,['Polaris catalog → Iceberg on RustFS','Service identity + vended credentials','Files may contain mixed permissions'],17)
d.line([(425,455),(496,455)])
d.lines(525,457,['Materialize rows allowed for the caller','Then run configured SQL / joins / windows','No unauthorized rows enter aggregates'],17)
d.line([(919,455),(977,455)])
d.lines(1004,457,['Recheck before returning','GraphQL and federated queries','Only authorized results leave'],17)
d.text(80,568,'No direct user access to Polaris or object-store credentials. The serving API enforces entity-level access.',18,TEAL,700)
d.line([(745,290),(745,352)],PURPLE,True)
d.tag(56,649,'02','THE SAME RULE APPLIES TO OTHER PATHS')
for x in [56,510,964]:d.rect(x,673,420,177,'#f5f7f8')
d.service(80,716,'Events + mutations','kafka',23);d.lines(80,752,['Authorize writes and validate the schema.','Publish scoped Kafka commands/events.','Subscriptions check each emission.'],17)
d.text(534,716,'Video + images',23,INK,700);d.lines(534,752,['Keep objects private in RustFS.','Authorize playlists, segments and ranges.','Check active transfers for revocation.'],17)
d.service(988,716,'AX + applications','substrate',23);d.lines(988,752,['Delegate scoped access or callbacks.','Keep the original user context.','Chat rechecks evidence before output.'],17)
d.tag(56,900,'03','DERIVED DATA RETAINS ITS SECURITY CONTEXT')
d.lines(56,938,['Trusted workspace and entity labels travel with events and projections. Application transformations must preserve','resource scope and provenance so query, subscription, media, and task paths can apply the same access decisions.'],19)
d.text(56,1021,'SERVICE BOUNDARIES',13,TEAL,700);d.text(259,1021,'Kafka ACLs  ·  Catalog grants  ·  Private buckets  ·  Kubernetes network policies  ·  Scoped service identities',17)
d.footer('Authorization is evaluated at access time. Cached data, stored projections, and retrieved evidence do not bypass the caller’s permissions.')
d.save()
print('Generated and rendered three SVG views plus compatible overview exports.')
