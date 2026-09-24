import tempfile, shutil, unittest
from pathlib import Path
from render import documents

class RenderTest(unittest.TestCase):
    def test_query_change_does_not_restart_processing(self):
        with tempfile.TemporaryDirectory() as folder:
            root=Path(folder)/'config';shutil.copytree('config',root)
            def names():return {r['metadata']['name'].split('-')[2]:r['metadata']['name'] for r in documents(root,'docker-desktop') if r['kind']=='ConfigMap'}
            before=names();p=root/'queries.yaml';p.write_text(p.read_text()+'\n# updated query\n');after=names()
            self.assertEqual(before['processor'],after['processor']);self.assertEqual(before['ingestion'],after['ingestion']);self.assertNotEqual(before['query'],after['query'])
    def test_all_config_mounts_preserve_schema_paths(self):
        resources=list(documents(Path('config'),'docker-desktop'))
        maps={r['metadata']['name']:r for r in resources if r['kind']=='ConfigMap'}
        for r in resources:
            if r['kind']=='FlinkDeployment':spec=r['spec']['podTemplate']['spec']
            elif r['kind']=='Deployment':spec=r['spec']['template']['spec']
            else:continue
            for v in spec.get('volumes',[]):
                if 'configMap' in v:
                    cm=v['configMap']
                    if cm['name']=='security-public-ca':continue
                    self.assertIn(cm['name'],maps)
                    for item in cm['items']:self.assertIn(item['key'],maps[cm['name']]['data'])

if __name__=='__main__':unittest.main()
