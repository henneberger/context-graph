import json
import tempfile
import unittest
from pathlib import Path
from context_harness.release import render


class NativeReleaseTest(unittest.TestCase):
    def test_taskmanager_does_not_inherit_jobmanager_http_probe(self):
        release = 'cg-sample-123456789abc'
        bundle = dict(release=release, name='sample', workspace='demo', namespace='sample',
                      digest='123456789abc', inputs={},
                      jobs=[dict(name=release+'-storage', parallelism=2)])
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory)
            (path/'bundle.json').write_text(json.dumps(bundle))
            manifests = render(path, 'docker-desktop')
        self.assertFalse(any(d['kind'] == 'FlinkDeployment' for d in manifests))
        deployments = {d['metadata']['name']: d for d in manifests if d['kind'] == 'Deployment'}
        for component in ('jm', 'tm'):
            container = deployments[release+'-storage-'+component]['spec']['template']['spec']['containers'][0]
            names = [e['name'] for e in container['env']]
            self.assertEqual(len(names), len(set(names)))
            self.assertEqual('readinessProbe' in container, component == 'jm')
            self.assertIn('JAVA_TOOL_OPTIONS', names)


if __name__ == '__main__':
    unittest.main()
