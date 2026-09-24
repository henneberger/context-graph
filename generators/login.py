#!/usr/bin/env python3
"""Obtain a short-lived development JWT and save it privately, without printing it."""
import argparse
import json
import os
from pathlib import Path
import tempfile
from load import connection


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--url',default='http://localhost:18088')
    parser.add_argument('--path',default='/auth/token')
    parser.add_argument('--username',default='producer')
    parser.add_argument('--credentials-file',default='.runtime/security/credentials.json')
    parser.add_argument('--token-file',required=True)
    parser.add_argument('--ca',default='.runtime/security/ca.crt')
    parser.add_argument('--allow-http',action='store_true')
    args=parser.parse_args()
    passwords=json.loads(Path(args.credentials_file).read_text())
    if args.username not in passwords:parser.error('User is absent from the private credentials file')
    conn,prefix=connection(args.url,15,args.ca,args.allow_http)
    try:
        conn.request('POST',prefix+args.path,json.dumps({'username':args.username,'password':passwords[args.username]}),{'Content-Type':'application/json'})
        response=conn.getresponse();body=response.read(32769)
        if response.status!=200 or len(body)>32768:raise RuntimeError('Identity login failed with HTTP '+str(response.status))
        result=json.loads(body)
        if not isinstance(result.get('access_token'),str):raise RuntimeError('Identity returned no access token')
    finally:conn.close()
    target=Path(args.token_file);target.parent.mkdir(parents=True,exist_ok=True,mode=0o700)
    descriptor,temporary=tempfile.mkstemp(prefix='.token-',dir=target.parent)
    try:
        with os.fdopen(descriptor,'w') as output:json.dump(result,output)
        os.chmod(temporary,0o600);os.replace(temporary,target)
    finally:
        if os.path.exists(temporary):os.unlink(temporary)
    print('Token response saved privately; lifetime '+str(result.get('expires_in','unknown'))+' seconds.')


if __name__=='__main__':main()
