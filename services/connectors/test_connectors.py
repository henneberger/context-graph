import importlib.util,unittest
from unittest.mock import patch,MagicMock
spec=importlib.util.spec_from_file_location('connectors','services/connectors/connectors.py');m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)
class ConnectorsTest(unittest.TestCase):
 def test_source_identity_uses_safe_stable_id_and_explicit_link(self):
  with patch.object(m,'database'),patch.object(m,'config',return_value={'workspace':'knowledge','identity_links':{'slack:T:U':['admin']}}),patch.object(m,'touch') as touch:
   principal=m.register_identity('slack','T:U','daniel')
   self.assertRegex(principal,r'^slack-[a-f0-9]{64}$')
   self.assertEqual(touch.call_args.args[0][1]['subject']['object']['objectId'],'admin')
 def test_github_paginates_without_following_untrusted_link(self):
  api=m.API.__new__(m.API);api.provider='github';r1=MagicMock(links={'next':{'url':'https://evil.invalid'}});r2=MagicMock(links={})
  api.get=MagicMock(side_effect=[([{'id':1}],r1),([{'id':2}],r2)])
  self.assertEqual([d['id'] for d in api.pages('repos/MariHQ/mari/issues')],[1,2]);self.assertTrue(all(c.args[0]=='repos/MariHQ/mari/issues' for c in api.get.call_args_list))
 def test_slack_repeated_cursor_fails_closed(self):
  api=m.API.__new__(m.API);api.provider='slack';api.get=MagicMock(return_value=({'messages':[],'response_metadata':{'next_cursor':'same'}},None))
  with self.assertRaises(m.ProviderError):list(api.pages('conversations.history',key='messages'))
 def test_slack_missing_page_is_not_complete(self):
  api=m.API.__new__(m.API);api.provider='slack';api.get=MagicMock(return_value=({'messages':[],'has_more':True},None))
  with self.assertRaises(m.ProviderError):list(api.pages('conversations.history',key='messages'))
 def test_citation_urls_are_source_bound(self):
  source={'provider':'github','name':'MariHQ/mari'}
  with self.assertRaises(m.ProviderError):m.make_document(source,'a','issue','d','title','body','https://evil.invalid/x',m.now())
 def test_expiring_acl_removes_old_readers_atomically(self):
  old=m.relation('source','github-id','reader','external_identity','github:old','account')
  response=MagicMock(text=m.json.dumps({'result':{'relationship':old}}))
  with patch.object(m,'spice',return_value=response) as spice,patch.object(m,'database'),patch.object(m,'config',return_value={'acl_lease_minutes':10}):
   m.set_acl('github-id',['github:new'])
   updates=spice.call_args_list[-1].args[1]['updates'];self.assertEqual(updates[0]['operation'],'OPERATION_DELETE');self.assertEqual(updates[1]['operation'],'OPERATION_TOUCH');self.assertIn('optionalExpiresAt',updates[1]['relationship'])
if __name__=='__main__':unittest.main()
