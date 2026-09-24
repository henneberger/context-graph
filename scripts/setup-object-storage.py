#!/usr/bin/env python3
"""Provision RustFS using its native rc client, with isolated service bucket policies."""
import json,subprocess,os
from pathlib import Path
import boto3
from botocore.config import Config
root=Path('.runtime/security');creds=json.loads((root/'lakehouse-credentials.json').read_text());rc=str(Path('.runtime/bin/rc').resolve())
def run(*args):
 p=subprocess.run([rc,'--quiet',*args],text=True,capture_output=True)
 if p.returncode:raise RuntimeError('RustFS administration failed: '+p.stderr[:400])
run('alias','set','context-admin',os.environ.get('OBJECT_STORAGE_SETUP_URL','https://localhost:19000'),creds['s3_root_key'],creds['s3_root_secret'],'--bucket-lookup','path','--ca-bundle',str((root/'ca.crt').resolve()))
try:
 s3=boto3.client('s3',endpoint_url=os.environ.get('OBJECT_STORAGE_SETUP_URL','https://localhost:19000'),aws_access_key_id=creds['s3_root_key'],aws_secret_access_key=creds['s3_root_secret'],verify=str(root/'ca.crt'),region_name='us-east-1',config=Config(s3={'addressing_style':'path'}))
 for role in ['warehouse','media','recovery']:
  bucket='context-'+role
  try:s3.head_bucket(Bucket=bucket)
  except s3.exceptions.ClientError as e:
   if e.response['ResponseMetadata']['HTTPStatusCode']!=404:raise
   s3.create_bucket(Bucket=bucket)
  policy={'Version':'2012-10-17','Statement':[{'Effect':'Allow','Action':['s3:ListBucket','s3:GetBucketLocation','s3:ListBucketMultipartUploads'],'Resource':['arn:aws:s3:::'+bucket]},{'Effect':'Allow','Action':['s3:GetObject','s3:PutObject','s3:DeleteObject','s3:AbortMultipartUpload','s3:ListMultipartUploadParts'],'Resource':['arn:aws:s3:::'+bucket+'/*']}]}
  if role=='warehouse':policy['Statement'].append({'Effect':'Allow','Action':['sts:AssumeRole'],'Resource':['arn:aws:s3:::*']})
  f=root/(role+'-policy.json');f.write_text(json.dumps(policy));f.chmod(0o600)
  run('admin','user','add','context-admin/','context-'+role,creds[role+'_secret'])
  run('admin','policy','create','context-admin/','context-'+role,str(f))
  run('admin','policy','attach','context-admin/','context-'+role,'--user','context-'+role)
finally:
 run('alias','remove','context-admin')
print('RustFS warehouse, media and recovery buckets and least-privilege users configured.')
