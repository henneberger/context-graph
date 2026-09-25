import importlib.util,json,unittest
from unittest.mock import patch
import server as m
DOC={'id':'d','revision':'one','url':'https://github.com/MariHQ/mari/issues/1','content':'evidence','source':'github'}
BLOCK={'text':'A grounded statement.','citations':['S1']}
class SearchTest(unittest.TestCase):
 def test_revision_or_permission_changes_abort(self):
  for rows in ([],[{**DOC,'revision':'two'}]):
   with patch.object(m,'gql',return_value={'documents':rows}):
    with self.assertRaises(m.Denied):m.recheck('Bearer valid',{'d':DOC})
 def test_decoder_waits_for_complete_cited_paragraph(self):
  decoder=m.BlockDecoder();self.assertEqual(decoder.feed('{"blocks":[{"text":"hello \\"world\\"","citations":['),[])
  self.assertEqual(decoder.feed('"S1"]}'),[{'text':'hello "world"','citations':['S1']}]);self.assertEqual(decoder.feed(']}'),[])
 def test_invalid_citation_never_emits_a_block(self):
  events=[]
  with patch.object(m,'search',return_value=[DOC]),patch.object(m,'recheck'),patch.object(m,'complete',return_value={}),patch.object(m,'completion_deltas',side_effect=lambda _:iter(['{"blocks":[{"text":"claim","citations":["invented"]}]}'])):
   with self.assertRaises(m.Denied):m.synthesize('Bearer valid','question',{'d':DOC},[],lambda k,v:events.append((k,v)))
  self.assertNotIn('block',[k for k,v in events]);self.assertEqual([k for k,v in events].count('reset'),1)
 def test_revocation_before_next_paragraph_stops_stream(self):
  events=[];checks=0
  def check(*args):
   nonlocal checks
   checks+=1
   if checks==3:raise m.Denied()
  def deltas(_):
   yield '{"blocks":['+json.dumps(BLOCK)
   yield ','+json.dumps(BLOCK)+']}'
  with patch.object(m,'search',return_value=[DOC]),patch.object(m,'recheck',side_effect=check),patch.object(m,'complete',return_value={}),patch.object(m,'completion_deltas',side_effect=deltas):
   with self.assertRaises(m.Denied):m.synthesize('Bearer valid','question',{'d':DOC},[],lambda k,v:events.append((k,v)))
  self.assertEqual([k for k,v in events].count('block'),1);self.assertNotIn('done',[k for k,v in events])
 def test_valid_block_is_sent_before_provider_finishes(self):
  events=[]
  def deltas(_):
   yield '{"blocks":['+json.dumps(BLOCK)
   self.assertEqual(events[-1][0],'block')
   yield ']}'
  with patch.object(m,'search',return_value=[DOC]),patch.object(m,'recheck'),patch.object(m,'complete',return_value={}),patch.object(m,'completion_deltas',side_effect=deltas):
   out=m.synthesize('Bearer valid','question',{'d':DOC},[],lambda k,v:events.append((k,v)))
  self.assertEqual(out['blocks'][0]['citations'],['d']);self.assertEqual(out['sources'][0]['url'],DOC['url']);self.assertNotIn('content',out['sources'][0]);self.assertEqual(events[-1][0],'done')
if __name__=='__main__':unittest.main()
