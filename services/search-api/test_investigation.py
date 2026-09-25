import hashlib,io,json,time,unittest
from unittest.mock import patch, Mock
from types import SimpleNamespace
import investigation as inv
import server
class Handler:
    def __init__(self,cap,path,body):
        raw=json.dumps(body).encode();self.headers={'Authorization':'Bearer '+cap,'Content-Length':str(len(raw))};self.path='/internal/investigation/'+path;self.rfile=io.BytesIO(raw);self.response=None;self.connection=type('Connection',(),{'settimeout':lambda *_:None})()
    def reply(self,status,value):self.response=(status,value)
class InvestigationTests(unittest.TestCase):
    def setUp(self):
        self.cap='test-capability';self.key=hashlib.sha256(self.cap.encode()).hexdigest();self.run=inv.Run('Bearer user-token',[{'query':'mari','source':'all','days':7}],time.monotonic()+60);inv.RUNS[self.key]=self.run
    def tearDown(self):inv.RUNS.clear()
    def test_unknown_or_expired_capability_denied(self):
        for cap in ('wrong',self.cap):
            if cap==self.cap:self.run.deadline=0
            with self.assertRaises(server.Denied):inv.callback(Handler(cap,'search',{'index':0}))
    def test_capability_cannot_change_query_or_call_twice(self):
        with self.assertRaises(server.Denied):inv.callback(Handler(self.cap,'search',{'index':0,'query':'secret'}))
        with patch.object(server,'search',return_value=[{'id':'a','revision':'1','content':'private','updatedAt':'today','score':1}]) as search,patch.object(server,'recheck'):
            h=Handler(self.cap,'search',{'index':0});inv.callback(h)
            search.assert_called_once_with('Bearer user-token','mari','all',6,7)
            self.assertNotIn('content',h.response[1]['documents'][0])
            with self.assertRaises(server.Denied):inv.callback(Handler(self.cap,'search',{'index':0}))
    def test_completion_requires_every_search_and_rechecks_access(self):
        with self.assertRaises(server.Denied):inv.callback(Handler(self.cap,'complete',{'indices':[0]}))
        self.run.used.add(0)
        with patch.object(server,'recheck',side_effect=server.Denied(403)):
            with self.assertRaises(server.Denied):inv.callback(Handler(self.cap,'complete',{'indices':[0]}))
        self.assertTrue(self.run.finished)
    def test_failed_task_is_detected_without_matching_other_runs(self):
        with patch.object(inv,'ax',return_value='NAME ATESPACE PHASE ACTOR WORKER-IP AGE\nrun-a context-search Failed actor <none> 1s\nrun-b context-search Running actor ip 1s\n'):
            self.assertTrue(inv.task_failed('run-a'))
            self.assertFalse(inv.task_failed('run-b'))
            self.assertFalse(inv.task_failed('run'))
    def test_capacity_waits_and_releases_slot_after_failure(self):
        slots=Mock();slots.acquire.side_effect=[False,True];events=[]
        with patch.object(inv,'TASK_SLOTS',slots),patch.object(server,'gql'),patch.object(inv,'execute_investigation',side_effect=server.Denied(503)):
            with self.assertRaises(server.Denied):inv.investigate('Bearer user',{'query':'mari'},lambda k,v:events.append((k,v)))
        self.assertEqual(events[0][1]['message'],'Waiting for research capacity')
        slots.release.assert_called_once()
    def test_both_answer_routes_delegate_only_to_ax(self):
        for route in ('/api/ask','/api/investigate'):
            body={'query':'mari','history':['What changed?']};raw=json.dumps(body).encode()
            handler=SimpleNamespace(path=route,headers={'Authorization':'Bearer user','Content-Length':str(len(raw))},rfile=io.BytesIO(raw),connection=Mock(),event=Mock(),failure=Mock(),reply=Mock())
            with patch.object(inv,'investigate') as execute,patch.object(server,'search') as search:
                server.Handler.do_POST(handler)
                execute.assert_called_once_with('Bearer user',body,handler.event)
                search.assert_not_called();handler.failure.assert_not_called()
    def test_missing_ax_fails_before_provider_call(self):
        with patch.dict('os.environ',{},clear=True),patch.object(server,'gql'),patch.object(server,'complete') as model:
            with self.assertRaises(server.Denied) as error:inv.investigate('Bearer user',{'query':'mari'},lambda *a:None)
            self.assertEqual(error.exception.code,'ax_unavailable');model.assert_not_called()
if __name__=='__main__':unittest.main()
