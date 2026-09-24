#!/usr/bin/env python3
"""Live check-only credential isolation test; never prints credentials."""
import base64,json
from pathlib import Path
import requests
items=json.loads(Path('.runtime/security/secrets.json').read_text())['items']
s=next(x for x in items if x['metadata']['name']=='spicedb-client')
client=base64.b64decode(s['data']['token']).decode()
headers={'Authorization':'Bearer '+client}
verify='.runtime/security/ca.crt'
body={'resource':{'object_type':'workspace','object_id':'demo'},'permission':'access','subject':{'object':{'object_type':'user','object_id':'alice'}}}
r=requests.post('https://localhost:18445/v1/permissions/check',headers=headers,json=body,verify=verify,timeout=10)
assert r.status_code==200 and r.json()['permissionship']=='PERMISSIONSHIP_HAS_PERMISSION',(r.status_code,'check failed')
for path in ['/v1/relationships/write','/v1/schema/write','/v1/permissions/check/../relationships/write','/v1/permissions/check?route=write']:
 r=requests.post('https://localhost:18445'+path,headers=headers,json={},verify=verify,timeout=10)
 assert r.status_code==404,(path,r.status_code)
r=requests.post('https://localhost:18443/v1/permissions/check',headers=headers,json=body,verify=verify,timeout=10)
assert r.status_code in (401,403),('API credential accepted by administrative endpoint',r.status_code)
r=requests.post('https://localhost:18445/v1/permissions/check',json=body,verify=verify,timeout=10)
assert r.status_code==401
print('PASS: API credential can check permissions, cannot mutate policy or authenticate directly to administrative SpiceDB; anonymous proxy denied')
