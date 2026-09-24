#!/usr/bin/env python3
"""Generate the editable, self-contained architectural overview (no external assets)."""
from pathlib import Path
from html import escape
import json
ROOT=Path(__file__).resolve().parent
W,H=2400,1920
parts=[]
def add(x): parts.append(x)
def rect(x,y,w,h,fill='#fff',stroke='#dbe3ed',r=20,extra=''):
 add(f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="{r}" fill="{fill}" stroke="{stroke}" {extra}/>')
def text(x,y,s,size=20,color='#142337',weight=400,extra=''):
 add(f'<text x="{x}" y="{y}" font-size="{size}" fill="{color}" font-weight="{weight}" {extra}>{escape(s)}</text>')
def lines(x,y,items,size=19,color='#536174',step=30,weight=400):
 for i,s in enumerate(items): text(x,y+i*step,s,size,color,weight)
def tag(x,y,label,color='#50617b',bg='#edf2f8',w=None):
 w=w or len(label)*9+24
 rect(x,y,w,27,bg,'none',8)
 text(x+12,y+19,label,13,color,700)
 return w
colors={'teal':'#087f8c','amber':'#a76609','violet':'#7050c8','blue':'#346ac5','red':'#b34e38','slate':'#52677f'}
def card(x,y,w,h,title,sub,color,ports=()):
 rect(x,y,w,h,extra='filter="url(#shadow)"')
 rect(x,y,5,h,color,'none',2)
 text(x+25,y+39,title,27,color,750)
 text(x+25,y+68,sub,15,'#69778b',500)
 px=x+w-24
 for label in reversed(ports):
  px-=34;tag(px,y+20,label,'#34495f','#edf2f8',27)
 add(f'<path d="M{x+25} {y+86} H{x+w-25}" stroke="#e6ebf2"/>')
def arrow(path,color='#58738f',dash=False,both=False):
 # White halo makes any route crossings unambiguous.
 add(f'<path d="{path}" fill="none" stroke="#f4f7fb" stroke-width="9" stroke-linejoin="round"/>')
 add(f'<path d="{path}" fill="none" stroke="{color}" stroke-width="2.8" stroke-linejoin="round" stroke-linecap="round" marker-end="url(#arrow-{color[1:]})"'+ (' stroke-dasharray="7 7"' if dash else '') + (f' marker-start="url(#start-{color[1:]})"' if both else '') + '/>')
def smallnote(x,y,title,items):
 text(x,y,title,15,'#40536c',750)
 lines(x,y+28,items,16,'#627086',25)

add(f'<svg xmlns="http://www.w3.org/2000/svg" width="{W}" height="{H}" viewBox="0 0 {W} {H}" role="img" aria-labelledby="title desc">')
add('<title id="title">Context Graph — complete system architecture</title>')
add('<desc id="desc">Local Kubernetes architecture: authenticated JSON, image and video ingestion through Vert.x; schema-validated Kafka; Flink graph and temporal processing; Iceberg and Kafka outputs; DuckDB GraphQL queries, Reactor Flux subscriptions, and a separate dashboard. OIDC and a check-only SpiceDB proxy enforce entity permissions. Separate media, recovery state, configuration and operator controls are shown. Matching lettered ports are explicit logical connections.</desc>')
add('<defs><filter id="shadow" x="-10%" y="-10%" width="120%" height="130%"><feDropShadow dx="0" dy="5" stdDeviation="8" flood-color="#203a59" flood-opacity="0.06"/></filter>')
for c in set(colors.values())|{'#58738f'}:
 add(f'<marker id="arrow-{c[1:]}" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse"><path d="M1 1 L9 5 L1 9" fill="none" stroke="{c}" stroke-width="1.7"/></marker>')
 add(f'<marker id="start-{c[1:]}" viewBox="0 0 10 10" refX="1" refY="5" markerWidth="7" markerHeight="7" orient="auto"><path d="M9 1 L1 5 L9 9" fill="none" stroke="{c}" stroke-width="1.7"/></marker>')
add('</defs><g font-family="Inter, Arial, sans-serif">')
rect(0,0,W,H,'#f4f7fb','none',0)
rect(60,50,10,76,colors['teal'],'none',4)
text(92,78,'CONTEXT / SYSTEM ARCHITECTURE',15,'#527089',750, 'letter-spacing="2.5"')
text(92,123,'A secure, live context graph',43,'#122438',750)
text(92,158,'From arbitrary JSON and streamed media to temporal metrics, connected entities, and authorized experiences.',21)
tag(1920,60,'LOCAL KUBERNETES',colors['blue'],'#e5eefc',360)
text(1920,113,'Namespace: context-graph',20,'#445773',600)
text(1920,145,'Implementation overview · 24 Sep 2026',16,'#69778b')

# SECURITY PLANE
rect(60,190,2220,340,'#fff5ef','#eedacb',24)
text(84,222,'01 / IDENTITY & AUTHORIZATION',15,colors['red'],750,'letter-spacing="1.5"')
text(790,222,'Workspace-wide reads • restricted entities need grants • writes require writer/admin',19,'#814d3f',600)
card(85,245,435,260,'OIDC identity','Local development issuer · 2 replicas',colors['red'],('A',))
lines(110,359,['RS256 JWT + HTTPS JWKS','APIs verify issuer, audience, subject, expiry','Development sign-in → 5-minute token'],17,step=29)
text(110,458,'Production: managed OIDC + browser PKCE¹',16,'#8b5b4c',600)
card(605,245,435,260,'Permission-check proxy','HTTPS · 2 replicas · separate API credential',colors['red'],('B',))
lines(630,359,['Only POST /v1/permissions/check','Forces fully consistent SpiceDB checks','No policy writes with API credentials'],18,step=30)
text(630,458,'Both Vert.x APIs connect through B',17,colors['red'],600)
card(1140,245,485,260,'SpiceDB','v1.56.2 · 2 replicas',colors['red'])
lines(1165,359,['Workspace membership + entity relations','view / ingest / manage permissions','No positive permission cache; fail closed'],18,step=30)
text(1165,458,'Restrict, grant and revoke through operator only',17,colors['red'],600)
card(1730,245,520,145,'PostgreSQL','Separate SpiceDB and Polaris databases',colors['red'],('P',))
text(1755,361,'Verified TLS connection · single instance¹',18)
rect(1730,414,520,91,'#fff','#eedacb',16)
text(1755,447,'Trusted operator tooling',23,colors['red'],700)
text(1755,478,'permissions.py • schema + grants • admin credential',17)
arrow('M1040 355 H1140',colors['red'],True)
arrow('M1625 322 H1730',colors['red'],True)
arrow('M1730 460 H1625',colors['red'],True)

# MAIN PIPELINE
text(60,575,'02 / INGEST, PROCESS & PERSIST',15,colors['teal'],750,'letter-spacing="1.5"')
text(925,575,'API, auth, Kafka, Polaris and S3 use verified TLS; identities and network policies limit access.',18,'#536174')
Y=610; HH=390
card(60,Y,280,HH,'Producers','Clients + load generators',colors['slate'],('A',))
lines(85,728,['Unstructured JSON','PNG / JPEG images','Streamed video'],23,'#243b53',42,600)
lines(85,884,['Rate + concurrency controls','Schema drift / time disorder','GOP and streaming controls'],17,step=29)
card(390,Y,360,HH,'Vert.x ingestion','v5.2 · 2 replicas · endpoint/topic config',colors['teal'],('A','B','C'))
lines(415,724,['JWT + workspace + ingest grant','Validate input and output JSON Schema','Derive canonical security labels'],17,step=29)
rect(413,814,313,100,'#edf8f8','none',12)
text(430,842,'Media processing',18,colors['teal'],700)
lines(430,869,['Image dimensions, hashes, metadata','FFmpeg → ~2s keyframe HLS chunks'],16,step=25)
text(415,953,'Acknowledge only after Kafka publish',16,colors['teal'],600)
tag(416,965,'M  /media permission gate',colors['teal'],'#e0f2f2',231)
tag(656,965,'E','#a76609','#fff4df',45)
card(800,Y,390,HH,'Kafka','4.1.1 · 3 brokers · replication 3 / min ISR 2',colors['amber'])
text(825,724,'ENDPOINT TOPICS',14,colors['amber'],750,'letter-spacing="1"')
lines(825,754,['cg.secure.events','cg.secure.images','cg.secure.video'],21,'#433721',30,600)
text(825,863,'VALIDATED JSON + MEDIA REFERENCES',13,'#7a6952',700)
add('<path d="M825 882 H1165" stroke="#eadfc9"/>')
text(825,909,'PROCESSED LIVE TOPICS',14,colors['amber'],750,'letter-spacing="1"')
text(825,941,'cg.secure.metrics / nodes / edges',18,'#433721',600)
text(825,974,'Input video topic also feeds subscriptions ↓',16,'#7a6952')
card(1240,Y,470,HH,'Flink stream processing','2.3 · Kafka connector + Iceberg sinks',colors['violet'],('C','R'))
lines(1265,725,['Revalidate schemas + canonical resource IDs','JSON Pointer projections and graph extraction','Event-time temporal aggregates + watermarks'],18,step=32)
rect(1263,826,423,87,'#f1edfb','none',12)
text(1280,855,'Security scope survives every transform',18,colors['violet'],700)
text(1280,884,'Keys: workspace + resource + metric / window',17)
lines(1265,946,['2 JobManagers (active / standby) • 1 TaskManager','Parallelism 2 · isolated candidate jobs supported'],16,step=25)
tag(1660,962,'E','#a76609','#fff4df',29)
card(1760,Y,520,HH,'Polaris + Iceberg','Polaris 1.7 · Iceberg 1.11 · RustFS object storage',colors['blue'],('P',))
text(1785,726,'Private catalog + warehouse',24,colors['blue'],650)
lines(1785,770,['Polaris REST · 2 replicas · persistent catalog P','RustFS S3 + STS · context-warehouse bucket','events / nodes / edges / metrics; labeled rows','Reader and writer identities are separate','Vended, temporary table-scoped credentials'],17,step=33)
text(1785,943,'Flink commits · governed API reads',18,colors['blue'],650)
text(1785,974,'Private backends; no direct user credentials',16,'#69778b')
arrow('M340 794 H390',colors['slate'])
arrow('M750 770 H800',colors['teal'])
arrow('M1190 770 H1240',colors['amber'])
arrow('M1240 938 H1190',colors['violet'])
arrow('M1710 770 H1760',colors['violet'])
text(1260,1041,'Checkpoint-aligned sinks; Kafka may lead committed Iceberg history.',17,'#6b5e8c')

# SERVING & MEDIA
text(60,1080,'03 / AUTHORIZED ACCESS',15,colors['teal'],750,'letter-spacing="1.5"')
card(60,1110,280,320,'Error queue','Operational / admin only',colors['amber'],('E',))
text(85,1225,'cg.secure.errors',21,colors['amber'],650)
lines(85,1264,['Schema / processing errors','Malformed / late records','Writers: ingestion + Flink','No GraphQL read access'],17,step=29)
text(85,1400,'Authorized failures only',16,'#7a6952')
card(390,1110,360,320,'Protected media store','RustFS context-media bucket + security sidecars',colors['teal'])
lines(415,1226,['Images, playlists and .ts chunks','File → workspace + entity binding','Only ingestion holds media keys'],18,step=31)
rect(413,1334,313,72,'#edf8f8','none',12)
lines(430,1362,['Media bytes never enter Kafka,','Flink, Iceberg or DuckDB.'],17,colors['teal'],26,600)
arrow('M570 1000 V1110',colors['teal'],False,True)
text(400,1051,'Complete chunks + security metadata',16,colors['teal'])
card(800,1110,910,320,'Vert.x GraphQL API','v5.2 · 2 replicas · only registered, labeled table schemas',colors['teal'],('A','B','C'))
rect(824,1216,402,189,'#f3f5fc','none',12)
text(843,1247,'LIVE / Kafka → Reactor Flux',20,colors['violet'],700)
lines(843,1279,['Unique consumer group per API replica','read_committed • bounded per-client queues','Topic / entity routing + permission per event','Expiry, revocation, cancellation, reconnect'],16,step=29)
rect(1242,1216,444,189,'#eef6fc','none',12)
text(1260,1247,'FEDERATED / DuckDB',20,colors['blue'],700)
lines(1260,1279,['iceberg + cache_httpfs · Polaris schema discovery','Authorize EACH source BEFORE joins / aggregates','Credential-free joins; recheck contributors','Edges require access to BOTH endpoints'],16,step=29)
card(1760,1110,520,320,'Context dashboard','Separate service · browser UI + Nginx proxy',colors['teal'],('A','M'))
text(1785,1227,'Live metrics   /   graph   /   HLS video',23,colors['teal'],650)
lines(1785,1267,['GraphQL HTTP + graphql-transport-ws','Bearer token + workspace on queries / WS init','/auth/session → Secure, HttpOnly media cookie','/media → ingestion permission gate M'],18,step=32)
text(1785,1409,'No direct file access · no data volume mounted',17,colors['teal'],650)
arrow('M1000 1000 V1110',colors['amber'])
text(1020,1068,'Live topics + video metadata',17,colors['amber'])
arrow('M2050 1000 V1060 H1500 V1110',colors['blue'])
text(1900,1088,'Schemas + table-scoped credentials',17,colors['blue'])
arrow('M1710 1280 H1760',colors['teal'])

# EXPLICIT KEYED CONNECTIONS / COMPLETE CONTROL PLANE
rect(60,1470,2220,64,'#e8f0f4','none',15)
text(84,1510,'M  MEDIA PATH',15,colors['teal'],750)
text(250,1510,'Browser / HLS.js ⇄ Nginx ⇄ ingestion authorization ⇄ RustFS media',20,'#24475a',600)
text(1130,1510,'Every file, playlist and range request rechecks access; active transfers check revocation and expiry.',17,'#436373')

text(60,1582,'04 / CONFIGURATION, RECOVERY & OPERATIONS',15,colors['slate'],750,'letter-spacing="1.5"')
card(60,1605,640,191,'JSON / YAML configuration','Versioned, content-addressed ConfigMaps',colors['slate'],('C',))
lines(85,1720,['ingestion.json + JSON Schemas → endpoint contracts','jobs.yaml → sources, transforms, windows, Kafka / Iceberg sinks','query.yaml + queries.yaml → schemas, SQL, GraphQL + routing'],17,step=28)
card(745,1605,715,191,'State & Flink orchestration','R  Operator 1.16.1 (2 replicas) ↔ FlinkDeployment / jobs',colors['violet'],('R',))
lines(770,1720,['Incremental RocksDB checkpoints · 10s interval / 5s min pause','Retained checkpoints + upgrade / hourly savepoints + HA metadata','RustFS recovery bucket · native savepoints · Kafka buffers input'],17,step=28)
card(1505,1605,775,191,'Deployment & network controls','Kubernetes Secrets, service identities and enforced NetworkPolicies',colors['slate'])
lines(1530,1720,['Firewall-only kube-router · API → check proxy; direct SpiceDB / PG blocked','Rolling replicas + PDBs + readiness + request drain; schema/ACL provisioning','Candidate Flink jobs: isolate state, groups, transactions, topics and tables'],17,step=28)
text(60,1834,'CONNECTOR KEY',13,'#536174',750,'letter-spacing="1"')
text(215,1834,'Solid → data / media     Dashed → control / authorization     Matching A / B / C / E / M / P / R ports are explicit connections, repeated to keep routes readable.',16,'#536174')
text(60,1875,'¹ LOCAL PROFILE LIMITS',14,colors['red'],750)
text(290,1875,'Single-node RustFS and single PostgreSQL are not HA. Savepoint upgrades pause job output. Production needs HA storage/DB, HTTPS ingress and OIDC PKCE.',16,'#705b54')
add('</g></svg>')
svg='\n'.join(parts)
(ROOT/'context-graph-architecture.svg').write_text(svg)

# Standalone interactive viewer; all assets are embedded.
html='''<!doctype html><html lang="en"><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>Context Graph · Architecture</title><style>
*{box-sizing:border-box}body{margin:0;background:#e8edf3;font:14px system-ui,sans-serif;color:#183047}header{height:70px;padding:0 25px;display:flex;align-items:center;gap:14px;background:#fff;border-bottom:1px solid #d5dfea}header b{font-size:17px}header span{color:#60738a;margin-right:auto}button{border:1px solid #ccd7e3;border-radius:8px;background:white;color:#24475a;padding:9px 13px;cursor:pointer}button:hover{background:#eef5fa}#viewport{height:calc(100vh - 70px);overflow:auto;padding:22px;cursor:grab}#viewport.dragging{cursor:grabbing;user-select:none}#diagram{margin:auto;width:2400px;box-shadow:0 12px 45px #203a5918}svg{display:block;width:100%;height:auto}#zoom{min-width:45px;text-align:center;font-variant-numeric:tabular-nums}@media(max-width:720px){header{padding:0 10px;gap:7px}header span{display:none}header b{margin-right:auto;font-size:13px}button{padding:8px}}@media print{header{display:none}#viewport{height:auto;padding:0;overflow:visible}#diagram{width:100%!important;box-shadow:none}body{background:#fff}}
</style><header><b>Context / Architecture</b><span>Pan, zoom and inspect the complete system</span><button id="minus" aria-label="Zoom out">−</button><output id="zoom"></output><button id="plus" aria-label="Zoom in">+</button><button id="fit">Fit</button><button id="download">Save SVG</button></header><main id="viewport"><div id="diagram">'''+svg+'''</div></main><script>
const viewport=document.getElementById('viewport'),diagram=document.getElementById('diagram');let scale=1;
function setScale(value){scale=Math.max(.2,Math.min(2.5,value));diagram.style.width=(2400*scale)+'px';document.getElementById('zoom').textContent=Math.round(scale*100)+'%';}
function fit(){setScale(Math.min((viewport.clientWidth-44)/2400,(viewport.clientHeight-44)/1920));viewport.scrollTo(0,0);}
document.getElementById('plus').onclick=()=>setScale(scale*1.2);document.getElementById('minus').onclick=()=>setScale(scale/1.2);document.getElementById('fit').onclick=fit;
document.getElementById('download').onclick=()=>{const blob=new Blob([new XMLSerializer().serializeToString(diagram.querySelector('svg'))],{type:'image/svg+xml'});const url=URL.createObjectURL(blob),a=document.createElement('a');a.href=url;a.download='context-graph-architecture.svg';a.click();setTimeout(()=>URL.revokeObjectURL(url),1000);};
let drag;viewport.addEventListener('pointerdown',e=>{if(e.button!==0)return;drag={x:e.clientX,y:e.clientY,left:viewport.scrollLeft,top:viewport.scrollTop};viewport.classList.add('dragging');viewport.setPointerCapture(e.pointerId);});viewport.addEventListener('pointermove',e=>{if(drag){viewport.scrollLeft=drag.left-e.clientX+drag.x;viewport.scrollTop=drag.top-e.clientY+drag.y;}});function end(){drag=null;viewport.classList.remove('dragging');}viewport.addEventListener('pointerup',end);viewport.addEventListener('pointercancel',end);viewport.addEventListener('wheel',e=>{if(e.ctrlKey||e.metaKey){e.preventDefault();setScale(scale*(e.deltaY<0?1.1:.9));}},{passive:false});window.addEventListener('resize',fit);fit();
</script></html>'''
(ROOT/'context-graph-architecture.html').write_text(html)
print('Generated SVG and standalone interactive HTML')
