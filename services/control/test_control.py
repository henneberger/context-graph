import importlib.util,time,unittest
from unittest.mock import patch
from cryptography.hazmat.primitives.asymmetric import rsa
import jwt
spec=importlib.util.spec_from_file_location('control','services/control/server.py');m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)
class ControlTest(unittest.TestCase):
 def setUp(self):
  import threading
  self.key=rsa.generate_private_key(public_exponent=65537,key_size=2048)
  self.auth=m.Authorization.__new__(m.Authorization);self.auth.tls=None;self.auth.keys={'test':self.key.public_key()};self.auth.fetched=time.monotonic();self.auth.lock=threading.Lock()
 def token(self,**changes):
  claims={'iss':m.ISSUER,'aud':m.AUDIENCE,'sub':'admin','exp':time.time()+300};claims.update(changes)
  return 'Bearer '+jwt.encode(claims,self.key,algorithm='RS256',headers={'kid':'test'})
 def test_valid_identity(self):self.assertEqual(self.auth.authenticate(self.token())['sub'],'admin')
 def test_invalid_identity(self):
  for value in (None,self.token(exp=time.time()-1),self.token(aud='wrong'),self.token(iss='https://attacker'),self.token(sub='*')):
   with self.assertRaises(m.Denied):self.auth.authenticate(value)
 def test_fully_consistent_separate_platform_grant_and_revocation(self):
  claims=self.auth.authenticate(self.token())
  with patch.dict(m.os.environ,{'SPICEDB_TOKEN':'test-only'}),patch.object(m,'fetch') as fetch:
   fetch.return_value={'permissionship':'PERMISSIONSHIP_HAS_PERMISSION'};self.auth.check(claims)
   body=fetch.call_args.args[1];self.assertEqual(body['resource'],{'object_type':'workspace','object_id':'platform'});self.assertEqual(body['permission'],'manage');self.assertTrue(body['consistency']['fully_consistent'])
   fetch.return_value={'permissionship':'PERMISSIONSHIP_NO_PERMISSION'}
   with self.assertRaises(m.Denied) as e:self.auth.check(claims)
   self.assertEqual(e.exception.status,403)
   fetch.side_effect=TimeoutError()
   with self.assertRaises(m.Denied) as e:self.auth.check(claims)
   self.assertEqual(e.exception.status,503)
 def test_expiry_rechecked(self):
  with self.assertRaises(m.Denied):self.auth.check({'sub':'admin','exp':0})
 def test_inventory_never_returns_pod_env(self):
  inventory=m.Inventory.__new__(m.Inventory)
  def listing(path):
   if path.endswith('/pods'):return [{'metadata':{'name':'p','creationTimestamp':'now'},'spec':{'containers':[{'name':'x','env':[{'name':'PASSWORD','value':'should-never-leak'}]}]},'status':{'phase':'Running'}}]
   return []
  inventory.listing=listing
  with patch.object(m,'fetch',side_effect=TimeoutError()):
   snapshot=inventory.snapshot();self.assertNotIn('should-never-leak',m.json.dumps(snapshot));self.assertEqual(snapshot['errors'],['metrics unavailable'])
 def test_rbac_no_mutations_secrets_logs_or_exec(self):
  resources=list(m.yaml.safe_load_all(m.Path('deploy/k8s/control.yaml').read_text()))
  role=next(r for r in resources if r['kind']=='Role')
  for rule in role['rules']:
   self.assertLessEqual(set(rule['verbs']),{'get','list'})
   self.assertFalse(set(rule['resources'])&{'secrets','pods/log','pods/exec','pods/proxy','services/proxy'})
if __name__=='__main__':unittest.main()
