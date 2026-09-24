#!/usr/bin/env python3
"""Live retrieval/filters/access checks; optional real DeepSeek answer. Never writes source bodies."""
import argparse,json,requests
from pathlib import Path
from kube_forward import service_forward
p=argparse.ArgumentParser();p.add_argument('--context',default='docker-desktop');p.add_argument('--ask',action='store_true');a=p.parse_args()
creds=json.loads(Path('.runtime/security/credentials.json').read_text());evidence={}
with service_forward(a.context,'dashboard',8080) as host:
 base='http://'+host
 def login(user):
  r=requests.post(base+'/auth/token',json={'username':user,'password':creds[user]},timeout=15);r.raise_for_status();return {'Authorization':'Bearer '+r.json()['access_token']}
 admin=login('admin')
 def search(body):
  r=requests.post(base+'/search/api/search',headers=admin,json=body,timeout=90);r.raise_for_status();return r.json()['results']
 rows=search({'query':'fix','source':'github'});assert rows and all(r['source']=='github' and r['container']=='MariHQ/mari' for r in rows)
 assert all(0<=r['recencyBoost']<=.35 and r['bm25Score']>0 for r in rows)
 for r in rows:assert abs(r['score']-r['bm25Score']*(1+r['recencyBoost'])*(1+r['titleBoost'])*r['typeWeight'])<1e-8
 evidence.update(realGitHubResults=True,repositoryScope=True,configuredRanking=True)
 rows=search({'query':'test','source':'slack'});assert rows and all(r['source']=='slack' for r in rows);evidence['realSlackResults']=True
 rows=search({'query':'fix','source':'github','sort':'recent'});assert [r['updatedAt'] for r in rows]==sorted([r['updatedAt'] for r in rows],reverse=True);evidence['newestSort']=True
 for headers in ({},login('alice')):
  r=requests.post(base+'/search/api/search',headers=headers,json={'query':'test'},timeout=90);assert r.status_code in (401,403)
 evidence['unauthorizedDenied']=True
 if a.ask:
  r=requests.post(base+'/search/api/ask',headers=admin,json={'query':'What recent fixes were made in Mari? Cite relevant commits or pull requests.','source':'github'},timeout=180);r.raise_for_status();answer=r.json();assert answer['blocks'] and answer['sources']
  ids={s['id'] for s in answer['sources']};assert all(set(b['citations'])<=ids for b in answer['blocks']);evidence['realDeepSeekAnswer']={'blocks':len(answer['blocks']),'sources':len(ids),'searches':len(answer['steps'])}
Path('docs/evidence/search.json').write_text(json.dumps(evidence,indent=2)+'\n');print('PASS live retrieval, ranking, filters and access checks'+(' with cited DeepSeek answer' if a.ask else ''))
