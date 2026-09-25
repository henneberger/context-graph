import tempfile,unittest
from pathlib import Path
from context_harness.compiler import sql_policy,Invalid,read_file
class ScopeTests(unittest.TestCase):
    def test_resource_local_projection_and_window(self):
        sql_policy("SELECT workspace_id,resource_id,entity_id,JSON_VALUE(payload,'$.text') AS text FROM events",{'events'})
        sql_policy("SELECT workspace_id,resource_id,entity_id,COUNT(*) AS n,TUMBLE_END(event_time,INTERVAL '10' SECOND) AS end_time FROM events GROUP BY workspace_id,resource_id,entity_id,TUMBLE(event_time,INTERVAL '10' SECOND)",{'events'})
    def test_permission_laundering_is_rejected(self):
        for sql in [
          "SELECT 'public' AS workspace_id,resource_id,entity_id,payload FROM events",
          "SELECT workspace_id,resource_id,entity_id,COUNT(*) AS n FROM events",
          "SELECT workspace_id,resource_id,entity_id,COUNT(*) AS n FROM events GROUP BY workspace_id",
          "SELECT workspace_id,resource_id,entity_id,COUNT(*) AS n FROM events GROUP BY ROLLUP(workspace_id,resource_id,entity_id)",
          "SELECT workspace_id,resource_id,entity_id,(SELECT count(*) FROM secret) AS n FROM events",
          "SELECT workspace_id,resource_id,entity_id FROM events JOIN secret ON true",
          "SELECT workspace_id,resource_id,entity_id FROM events UNION SELECT workspace_id,resource_id,entity_id FROM secret",
          "SELECT workspace_id,resource_id,entity_id,SUM(value) OVER () AS n FROM events",
          "SELECT workspace_id,resource_id,entity_id FROM events; DROP TABLE events",
          "SELECT workspace_id,resource_id,entity_id FROM read_csv('/secret')",
          "SELECT workspace_id,resource_id,entity_id,evil(payload) AS n FROM events",
        ]:
            with self.subTest(sql=sql),self.assertRaises(Invalid):sql_policy(sql,{'events'})
    def test_symlink_escape_is_rejected(self):
        with tempfile.TemporaryDirectory() as d:
            root=Path(d)/'bundle';root.mkdir();secret=Path(d)/'secret';secret.write_text('private');(root/'schema').symlink_to(secret)
            with self.assertRaises(Invalid):read_file(root,'schema')
if __name__=='__main__':unittest.main()
