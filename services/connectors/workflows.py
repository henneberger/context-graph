from datetime import timedelta
from temporalio import workflow
from temporalio.common import RetryPolicy

@workflow.defn
class SourceSync:
    @workflow.run
    async def run(self,source_id:str):
        return await workflow.execute_activity('sync_source',source_id,start_to_close_timeout=timedelta(minutes=20),heartbeat_timeout=timedelta(seconds=60),retry_policy=RetryPolicy(initial_interval=timedelta(seconds=10),maximum_interval=timedelta(minutes=5),maximum_attempts=5))

@workflow.defn
class ConnectorSweep:
    @workflow.run
    async def run(self,provider:str):
        import asyncio
        ids=await workflow.execute_activity('discover_sources',provider,start_to_close_timeout=timedelta(minutes=10),heartbeat_timeout=timedelta(seconds=60))
        results=[]
        # Bounded child workflow fan-out; document contents and credentials never enter history.
        for offset in range(0,len(ids),4):
            handles=[await workflow.start_child_workflow(SourceSync.run,identifier,id='source-'+identifier+'-'+workflow.info().run_id,execution_timeout=timedelta(hours=2)) for identifier in ids[offset:offset+4]]
            for result in await asyncio.gather(*handles,return_exceptions=True):results.append(result if isinstance(result,dict) else {'failed':True})
        return {'sources':len(ids),'completed':sum(not r.get('failed') for r in results)}

@workflow.defn
class RefreshPermissions:
    @workflow.run
    async def run(self,provider:str):
        return await workflow.execute_activity('refresh_permissions',provider,start_to_close_timeout=timedelta(minutes=20),heartbeat_timeout=timedelta(seconds=60),retry_policy=RetryPolicy(maximum_attempts=3))
