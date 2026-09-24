#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
context="${KUBE_CONTEXT:?Set KUBE_CONTEXT explicitly, for example docker-desktop}"
node="${STORAGE_NODE:?Set STORAGE_NODE to the single shared-storage node hostname}"
python="${PYTHON:-python3}"
mkdir -p .runtime
if ! "$python" scripts/check-network-policy.py --context "$context"; then
  kubectl --context "$context" apply -f deploy/kube-router-firewall.yaml
  kubectl --context "$context" -n kube-system rollout status daemonset/context-network-policy --timeout=180s
  "$python" scripts/check-network-policy.py --context "$context"
fi
kubectl --context "$context" get node "$node" >/dev/null
kubectl --context "$context" apply -f deploy/k8s/namespace.yaml
"$python" scripts/bootstrap-security.py --context "$context"
# Apply namespace isolation before starting any new data service.
"$python" - <<'PYNP' > .runtime/base-isolation.yaml
import yaml
from pathlib import Path
print(yaml.safe_dump_all(d for d in yaml.safe_load_all(Path('deploy/k8s/network-policies.yaml').read_text()) if d and d['metadata']['name'] in ('default-deny','dns-egress')))
PYNP
kubectl --context "$context" apply -f .runtime/base-isolation.yaml
# Prepare the persistent database before Polaris bootstrap and service credentials.
"$python" - <<'PYDB' > .runtime/postgres.yaml
import yaml
from pathlib import Path
docs=[d for d in yaml.safe_load_all(Path('deploy/k8s/security-services.yaml').read_text()) if d and d['metadata']['name']=='spicedb-postgres']
print(yaml.safe_dump_all(docs,sort_keys=False))
PYDB
kubectl --context "$context" apply -f .runtime/postgres.yaml
kubectl --context "$context" -n context-graph rollout status statefulset/spicedb-postgres --timeout=180s
"$python" scripts/deploy-lakehouse.py --context "$context" --node "$node"
"$python" scripts/provision-control.py --context "$context"
"$python" scripts/provision-connectors.py --context "$context"
chart=.runtime/flink-operator-1.16.1.tgz
if [[ ! -f "$chart" || ! -f "$chart.sha512" ]]; then
  curl -fsSL https://downloads.apache.org/flink/flink-kubernetes-operator-1.16.1/flink-kubernetes-operator-1.16.1-helm.tgz -o "$chart"
  curl -fsSL https://downloads.apache.org/flink/flink-kubernetes-operator-1.16.1/flink-kubernetes-operator-1.16.1-helm.tgz.sha512 -o "$chart.sha512"
fi
"$python" - "$chart" <<'PY'
import hashlib,pathlib,sys
p=pathlib.Path(sys.argv[1]); expected=p.with_name(p.name+'.sha512').read_text().split()[0]
assert hashlib.sha512(p.read_bytes()).hexdigest()==expected,'Helm chart checksum mismatch'
PY
helm upgrade --install context-flink-operator "$chart" --kube-context "$context" --namespace context-graph --values deploy/operator-values.yaml --wait --timeout 180s
"$python" - "$context" "$node" "${IMAGE_TAG:-secure-v6}" > .runtime/rendered.yaml <<'PYTHON'
import ipaddress,json,subprocess,sys,yaml
sys.path.insert(0,'scripts')
from render import documents
context,node,tag=sys.argv[1:]
def get(resource):return json.loads(subprocess.check_output(['kubectl','--context',context,'-n','default','get',resource,'kubernetes','-o','json']))
ips=get('service')['spec']['clusterIPs']
ips += [a['ip'] for subset in get('endpoints').get('subsets',[]) for a in subset.get('addresses',[])]
cidrs=[str(ipaddress.ip_network(ip+'/128' if ':' in ip else ip+'/32')) for ip in sorted(set(ips))]
print(yaml.safe_dump_all(documents(node=node,image_tag=tag,api_cidrs=cidrs),sort_keys=False))
PYTHON
# Enforce boundaries before starting the data services.
"$python" - <<'PYTHON'
import yaml
from pathlib import Path
docs=list(yaml.safe_load_all(Path('.runtime/rendered.yaml').read_text()))
Path('.runtime/policies.yaml').write_text(yaml.safe_dump_all([d for d in docs if d['kind']=='NetworkPolicy']))
PYTHON
kubectl --context "$context" apply -f .runtime/policies.yaml
"$python" scripts/apply-rendered.py --context "$context" --manifest .runtime/rendered.yaml
kubectl --context "$context" -n context-graph rollout status statefulset/spicedb-postgres --timeout=300s
kubectl --context "$context" -n context-graph wait --for=condition=complete job/spicedb-migrate-v1562 --timeout=300s
kubectl --context "$context" -n context-graph rollout status statefulset/secure-kafka --timeout=300s
kubectl --context "$context" -n context-graph wait --for=condition=complete job/secure-topics-v1 --timeout=180s
kubectl --context "$context" -n context-graph rollout status statefulset/temporal --timeout=300s
for service in spicedb spicedb-checks identity ingestion query control prometheus otel-collector postgres-exporter dashboard control-ui search-api search-ui connectors; do
  kubectl --context "$context" -n context-graph rollout status "deployment/$service" --timeout=300s
done
"$python" scripts/wait-flink.py --context "$context" --name context-secure-v1
"$python" scripts/check-service-boundaries.py --context "$context"
kubectl --context "$context" -n context-graph get flinkdeployment,pods
