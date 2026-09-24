import importlib.util,unittest
from unittest.mock import patch
spec=importlib.util.spec_from_file_location('search_api','services/search-api/server.py');m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)
DOC={'id':'d','revision':'one','url':'https://github.com/MariHQ/mari/issues/1','content':'evidence','source':'github'}
class SearchTest(unittest.TestCase):
 def test_revision_or_permission_changes_abort(self):
  with patch.object(m,'gql',return_value={'documents':[]}):
   with self.assertRaises(m.Denied):m.recheck('Bearer valid',{'d':DOC})
  with patch.object(m,'gql',return_value={'documents':[{**DOC,'revision':'two'}]}):
   with self.assertRaises(m.Denied):m.recheck('Bearer valid',{'d':DOC})
 def test_hallucinated_citation_is_rejected(self):
  with patch.object(m,'search',return_value=[DOC]),patch.object(m,'recheck'),patch.object(m,'complete',return_value={'content':'{"blocks":[{"text":"claim","citations":["invented"]}]}'}):
   with self.assertRaises(m.Denied) as e:m.answer('Bearer valid','question')
   self.assertEqual(e.exception.code,'invalid_assistant_citation')
 def test_revocation_during_generation_prevents_answer(self):
  with patch.object(m,'search',return_value=[DOC]),patch.object(m,'recheck',side_effect=[None,m.Denied()]),patch.object(m,'complete',return_value={'content':'{"blocks":[{"text":"claim","citations":["d"]}]}'}):
   with self.assertRaises(m.Denied):m.answer('Bearer valid','question')
 def test_citations_come_only_from_authorized_results(self):
  with patch.object(m,'search',return_value=[DOC]),patch.object(m,'recheck') as check,patch.object(m,'complete',return_value={'content':'{"blocks":[{"text":"claim","citations":["d"]}]}'}):
   out=m.answer('Bearer valid','question');self.assertEqual(out['sources'][0]['url'],DOC['url']);self.assertNotIn('content',out['sources'][0]);self.assertEqual(check.call_count,2)
if __name__=='__main__':unittest.main()
