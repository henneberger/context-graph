import importlib.util
from pathlib import Path
import unittest
from unittest.mock import patch

spec=importlib.util.spec_from_file_location('permissions',Path(__file__).with_name('permissions.py'))
permissions=importlib.util.module_from_spec(spec)
spec.loader.exec_module(permissions)

class ProvisionerTests(unittest.TestCase):
    def test_creation_requires_atomic_absence_precondition(self):
        with patch.object(permissions,'workspace_bindings',return_value=[]),patch.object(permissions,'api') as api:
            permissions.ensure_entity_workspace('demo','alpha',create=True)
            payload=api.call_args.args[1]
            self.assertEqual('OPERATION_MUST_NOT_MATCH',payload['optionalPreconditions'][0]['operation'])
            self.assertEqual('workspace',payload['optionalPreconditions'][0]['filter']['optionalRelation'])
            self.assertEqual('OPERATION_CREATE',payload['updates'][0]['operation'])

    def test_reassociation_denied_and_existing_binding_unchanged(self):
        rid=permissions.resource_id('demo','alpha')
        with patch.object(permissions,'workspace_bindings',return_value=[permissions.relationship('entity',rid,'workspace','workspace','other')]):
            with self.assertRaises(RuntimeError):permissions.ensure_entity_workspace('demo','alpha',create=True)
        with patch.object(permissions,'workspace_bindings',return_value=[permissions.relationship('entity',rid,'workspace','workspace','demo')]),patch.object(permissions,'api') as api:
            self.assertEqual(rid,permissions.ensure_entity_workspace('demo','alpha',create=True));api.assert_not_called()

    def test_no_direct_workspace_relationship_write_or_implicit_grant(self):
        item=permissions.relationship('entity',permissions.resource_id('demo','alpha'),'workspace','workspace','demo')
        with self.assertRaises(ValueError):permissions.write([item])
        with patch.object(permissions,'workspace_bindings',return_value=[]):
            with self.assertRaises(RuntimeError):permissions.ensure_entity_workspace('demo','alpha')

if __name__=='__main__':unittest.main()
