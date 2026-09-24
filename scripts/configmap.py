#!/usr/bin/env python3
"""Render a ConfigMap and volume items preserving nested config/schema paths."""
import argparse, json
from pathlib import Path

def render(root, name):
    files=sorted(p for p in root.rglob('*') if p.is_file())
    keys={str(p.relative_to(root)):str(p.relative_to(root)).replace('/', '__') for p in files}
    if len(set(keys.values())) != len(keys): raise ValueError('ConfigMap key collision')
    return {'apiVersion':'v1','kind':'ConfigMap','metadata':{'name':name,'namespace':'context-graph'},'data':{keys[str(p.relative_to(root))]:p.read_text() for p in files}}, [{'key':key,'path':path} for path,key in keys.items()]

if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('--root',type=Path,default=Path('config'));parser.add_argument('--name',default='context-config');parser.add_argument('--items',action='store_true');args=parser.parse_args()
    config,items=render(args.root,args.name);print(json.dumps(items if args.items else config,indent=2))
