import argparse
import importlib.util
from pathlib import Path
import tempfile
import unittest
import yaml

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location('version_job', ROOT / 'scripts/version-job.py')
version_job = importlib.util.module_from_spec(spec)
spec.loader.exec_module(version_job)


class VersionJobTest(unittest.TestCase):
    def test_isolated_tables_topics_groups_and_source_replay(self):
        original = version_job.read(ROOT / 'config/jobs.yaml')
        queries = version_job.read(ROOT / 'config/queries.yaml')
        job, mapped, tables, topics = version_job.version_config(original, queries, 'v2')
        self.assertEqual(job['sourceTopics'], original['sourceTopics'])
        self.assertNotEqual(job['consumerGroup'], original['consumerGroup'])
        self.assertFalse(set(tables) & set(tables.values()))
        self.assertFalse(set(topics) & set(topics.values()))
        self.assertEqual(mapped['queries']['metrics']['table'], 'context_secure_v2.metrics')
        self.assertEqual(mapped['subscriptions']['metricUpdated']['topics'], ['cg.secure.metrics.v2'])
        self.assertEqual(mapped['subscriptions']['videoChunk']['topics'], ['cg.secure.video'])
        self.assertEqual(original['name'], 'context-secure-v1')

    def test_invalid_version_and_duplicate_version_fail(self):
        job = version_job.read(ROOT / 'config/jobs.yaml')
        queries = version_job.read(ROOT / 'config/queries.yaml')
        for version in ['v1', '../../etc', 'v2;echo', 'v2_', 'v2' + 'x' * 25]:
            with self.assertRaises(ValueError):
                version_job.version_config(job, queries, version)

    def test_real_manifests_isolate_candidate_until_explicit_cutover(self):
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / 'v2'
            args = argparse.Namespace(version='v2', output=str(output), node='test-node',
                job_config=ROOT / 'config/jobs.yaml', query_config=ROOT / 'config/queries.yaml',
                flink_manifest=ROOT / 'deploy/k8s/flink.yaml', apps_manifest=ROOT / 'deploy/k8s/apps.yaml',
                topics_manifest=ROOT / 'deploy/k8s/secure-topics.yaml')
            version_job.render(args)
            flink = version_job.read(output / 'flink.yaml')
            self.assertEqual(flink['metadata']['name'], 'context-secure-v2')
            template = flink['spec']['podTemplate']['spec']
            env = {item['name']: item.get('value') for item in template['containers'][0]['env']}
            self.assertEqual(env['JOB_ID'], 'context-secure-v2')
            self.assertTrue(env['CHECKPOINT_URI'].endswith('/context-secure-v2'))
            self.assertTrue(env['SAVEPOINT_URI'].endswith('/context-secure-v2'))
            deployment, service, pdb = list(yaml.safe_load_all((output / 'query.yaml').read_text()))
            self.assertEqual(service['spec']['selector'], {'app': 'query-v2', 'release': 'v2', 'context-graph-role':'query'})
            self.assertNotEqual(deployment['spec']['template']['metadata']['labels']['app'], 'query')
            self.assertEqual(deployment['spec']['template']['spec']['nodeSelector']['kubernetes.io/hostname'], 'test-node')
            configmap = version_job.read(output / 'configmap.yaml')
            self.assertIn('schemas__envelope.json', configmap['data'])
            self.assertIn('WARMUP_VERIFIED', (output / 'cutover.sh').read_text())
            topics=version_job.read(output / 'topics.yaml')
            command=topics['spec']['template']['spec']['containers'][0]['args'][0]
            self.assertIn('--command-config /app/secrets/kafka.properties', command)
            self.assertIn('--allow-principal User:query --operation Read --operation Describe --topic cg.secure.metrics.v2',command)
            self.assertNotIn('--allow-principal User:query --operation Read --operation Describe --topic cg.secure.errors.v2',command)
            self.assertIn('KUBE_CONTEXT', (output / 'cutover.sh').read_text())
            with self.assertRaises(ValueError):
                version_job.render(args)


if __name__ == '__main__':
    unittest.main()
