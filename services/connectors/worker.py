import asyncio,concurrent.futures,json,os,signal,threading
from datetime import timedelta
from http.server import BaseHTTPRequestHandler,ThreadingHTTPServer
from pathlib import Path
from temporalio.client import Client,Schedule,ScheduleActionStartWorkflow,ScheduleSpec,ScheduleIntervalSpec,SchedulePolicy,ScheduleOverlapPolicy,ScheduleAlreadyRunningError,ScheduleUpdate,TLSConfig
from temporalio.worker import Worker
from prometheus_client import start_http_server
from connectors import initialize,config,database,discover_sources,refresh_permissions,sync_source
from workflows import ConnectorSweep,RefreshPermissions,SourceSync

READY=False

class Status(BaseHTTPRequestHandler):
    def log_message(self,*args):pass
    def do_GET(self):
        if self.path not in ('/health/live','/status'):self.send_error(404);return
        try:
            if self.path=='/health/live' and not READY:self.send_error(503);return
            result={'status':'ok'}
            if self.path=='/status':
                with database() as db:
                    rows=db.execute('SELECT provider,name,status,documents,last_success,acl_verified,error FROM sources ORDER BY provider,name').fetchall()
                    result={'sources':[dict(zip(('provider','name','status','documents','lastSuccess','aclVerified','error'),row)) for row in rows],'identities':db.execute('SELECT count(*) FROM identities').fetchone()[0]}
            data=json.dumps(result,default=str).encode();self.send_response(200);self.send_header('Content-Type','application/json');self.send_header('Content-Length',str(len(data)));self.end_headers();self.wfile.write(data)
        except Exception:self.send_error(503)

async def main():
    global READY
    initialize();start_http_server(9404)
    threading.Thread(target=ThreadingHTTPServer(('0.0.0.0',9406),Status).serve_forever,daemon=True).start()
    root=Path('/run/connectors')
    client=await Client.connect(os.environ.get('TEMPORAL_ADDRESS','temporal:7233'),namespace='context-connectors',tls=TLSConfig(server_root_ca_cert=(root/'ca.crt').read_bytes(),client_cert=(root/'server.crt').read_bytes(),client_private_key=(root/'server.key').read_bytes(),domain='temporal'))
    cfg=config()
    for provider in cfg['providers']:
        for mode,workflow,minutes in [('content',ConnectorSweep,cfg['content_sync_minutes']),('acl',RefreshPermissions,cfg['acl_sync_minutes'])]:
            schedule=Schedule(action=ScheduleActionStartWorkflow(workflow.run,provider,id='connector-'+provider+'-'+mode,task_queue='context-connectors'),spec=ScheduleSpec(intervals=[ScheduleIntervalSpec(every=timedelta(minutes=minutes))]),policy=SchedulePolicy(overlap=ScheduleOverlapPolicy.SKIP))
            try:await client.create_schedule('connector-'+provider+'-'+mode,schedule,trigger_immediately=True)
            except ScheduleAlreadyRunningError:
                def update(existing):
                    schedule.state=existing.description.schedule.state
                    return ScheduleUpdate(schedule=schedule)
                await client.get_schedule_handle('connector-'+provider+'-'+mode).update(update)
    stop=asyncio.Event();loop=asyncio.get_running_loop()
    for sig in (signal.SIGTERM,signal.SIGINT):loop.add_signal_handler(sig,stop.set)
    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
        async with Worker(client,task_queue='context-connectors',workflows=[ConnectorSweep,RefreshPermissions,SourceSync],activities=[discover_sources,refresh_permissions,sync_source],activity_executor=pool,max_concurrent_activities=4,max_concurrent_workflow_tasks=4,graceful_shutdown_timeout=timedelta(seconds=40)):
            READY=True
            await stop.wait()
            READY=False
if __name__=='__main__':asyncio.run(main())
