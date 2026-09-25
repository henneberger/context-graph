import argparse,json,sys
from pathlib import Path
from .compiler import compile_bundle,Invalid

def main():
    p=argparse.ArgumentParser(description='Context Graph application harness')
    sub=p.add_subparsers(dest='command',required=True)
    for command in ('validate','build'):
        cmd=sub.add_parser(command);cmd.add_argument('bundle');cmd.add_argument('--output',required=True)
    cmd=sub.add_parser('init');cmd.add_argument('directory')
    cmd=sub.add_parser('inspect');cmd.add_argument('release')
    cmd=sub.add_parser('render');cmd.add_argument('release');cmd.add_argument('--node',required=True)
    cmd=sub.add_parser('deploy');cmd.add_argument('release');cmd.add_argument('--context',required=True);cmd.add_argument('--node',required=True)
    cmd=sub.add_parser('status');cmd.add_argument('release');cmd.add_argument('--context',required=True)
    cmd=sub.add_parser('promote');cmd.add_argument('release');cmd.add_argument('--context',required=True)
    args=p.parse_args()
    try:
        if args.command in ('validate','build'):
            result=compile_bundle(args.bundle,args.output);print(json.dumps({'release':result['release'],'jobs':len(result['jobs']),'directory':args.output}))
        elif args.command=='init':
            import shutil
            from .compiler import ROOT
            shutil.copytree(ROOT/'examples/group-insights',args.directory);print('Created editable bundle at '+args.directory)
        elif args.command=='inspect':
            b=json.loads((Path(args.release)/'bundle.json').read_text());print(json.dumps({k:b[k] for k in ('release','workspace','namespace','inputs','jobs')},indent=2))
        elif args.command=='render':
            from .release import render
            import yaml
            print(yaml.safe_dump_all(render(Path(args.release),args.node),sort_keys=False))
        else:
            from .release import deploy,status,promote
            {'deploy':deploy,'status':status,'promote':promote}[args.command](args)
    except (Invalid,ValueError) as e:p.exit(2,str(e)+'\n')
