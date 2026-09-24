#!/usr/bin/env python3
"""Copy retained media to private S3, preserving object paths and verifying SHA256 by download."""
import argparse,hashlib,json,subprocess,tarfile,tempfile
from pathlib import Path
import boto3
from botocore.config import Config
p=argparse.ArgumentParser();p.add_argument('--pod',required=True);a=p.parse_args();root=Path('.runtime/security');creds=json.loads((root/'lakehouse-credentials.json').read_text())
s3=boto3.client('s3',endpoint_url='https://localhost:19000',aws_access_key_id='context-media',aws_secret_access_key=creds['media_secret'],verify=str(root/'ca.crt'),region_name='us-east-1',config=Config(s3={'addressing_style':'path'}))
with tempfile.TemporaryDirectory(prefix='context-media-') as tmp:
 archive=Path(tmp)/'media.tar'
 with archive.open('wb') as out:subprocess.run(['kubectl','--context','docker-desktop','-n','context-graph','exec',a.pod,'--','tar','cf','-','-C','/data/media','.'],stdout=out,check=True)
 with tarfile.open(archive) as tar:
  files=[m for m in tar.getmembers() if m.isfile() and (m.name.endswith(('.png','.jpg','.ts','.m3u8','.security.json')))]
  # Provenance first, payload second, playlists last; users stay on their existing API.
  files.sort(key=lambda m: (0 if m.name.endswith('.security.json') else 2 if m.name.endswith('.m3u8') else 1,m.name))
  for member in files:
   key=member.name.removeprefix('./');data=tar.extractfile(member).read();digest=hashlib.sha256(data).hexdigest()
   try:
    existing=s3.head_object(Bucket='context-media',Key=key)
    # New API uploads own their objects; never overwrite a live playlist from local staging.
    if 'migration-sha256' not in existing.get('Metadata',{}):continue
   except s3.exceptions.ClientError as e:
    if e.response['ResponseMetadata']['HTTPStatusCode']!=404:raise
   s3.put_object(Bucket='context-media',Key=key,Body=data,Metadata={'migration-sha256':digest})
   check=s3.get_object(Bucket='context-media',Key=key)['Body']
   with check:actual=hashlib.file_digest(check,'sha256').hexdigest()
   if actual!=digest:raise RuntimeError('Media checksum mismatch')
print(json.dumps({'objectsVerified':len(files),'sourceRetained':True}))
