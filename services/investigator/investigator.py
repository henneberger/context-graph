"""Execute a bounded retrieval plan through a per-run read capability."""
import base64,json,os,ssl
import urllib3

def main():
    context=ssl.create_default_context(cadata=base64.b64decode(os.environ['CALLBACK_CA']).decode())
    pool=urllib3.HTTPSConnectionPool(os.environ['CALLBACK_IP'],port=int(os.environ.get('CALLBACK_PORT','8443')),ssl_context=context,server_hostname=os.environ['CALLBACK_TLS_NAME'],assert_hostname=os.environ['CALLBACK_TLS_NAME'],timeout=urllib3.Timeout(connect=5,read=45),retries=False)
    def call(path,body):
        r=pool.request('POST','/internal/investigation/'+path,body=json.dumps(body).encode(),headers={'Authorization':'Bearer '+os.environ['RUN_CAPABILITY'],'Content-Type':'application/json'},redirect=False)
        if r.status!=200:raise RuntimeError('Task access denied or retrieval unavailable')
        return json.loads(r.data)
    count=int(os.environ['SEARCH_COUNT'])
    if not 1<=count<=5:raise ValueError('Invalid search bound')
    try:
        for index in range(count):call('search',{'index':index})
        call('complete',{'indices':list(range(count))})
    finally:pool.close()
if __name__=='__main__':main()
