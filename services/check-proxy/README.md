# Read-only SpiceDB check proxy

APIs receive a dedicated check credential and call `https://spicedb-checks:8443/v1/permissions/check`. They never receive the SpiceDB administrative preshared key. The proxy accepts only the exact POST permission-check route, overwrites consistency to fully consistent, and strips responses to permissionship. Schema, relationship writes, alternate routes and methods are unavailable. Both incoming and backend transport use verified TLS. Kubernetes policy permits API traffic to this proxy and permits only the proxy to reach SpiceDB.

The `check-proxy-security` Secret contains a distinct `client-token`, `backend-token`, server certificate/key and public CA. Only the proxy mounts the administrative backend token. APIs mount `spicedb-client`, containing the read-only credential. Trusted operator tooling separately reads the administrative token from a private local file.

`scripts/provision-check-proxy.py` prepares certificates and updates `.runtime/security/secrets.json` so the API secret cannot revert to an administrative credential when bootstrap is rerun. `--context` optionally applies the generated Secrets. `bootstrap-security.py` invokes this helper on every run, including reuse. Files containing private material use mode 0600 and are not printed.

`scripts/test-check-proxy.py` exercises real permission checks, anonymous denial, route/schema/write denial and rejection of the API token by administrative SpiceDB endpoints. `scripts/security-outage-smoke.py --context docker-desktop` deliberately changes the proxy backend to an unavailable DNS name, verifies APIs fail closed, and restores the original backend in a finally block. Run that destructive availability experiment only against the authorized local development deployment.
