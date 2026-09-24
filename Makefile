.PHONY: test build deploy render dashboard

test:
	mvn test
	npm --prefix services/dashboard ci
	npm --prefix services/dashboard test
	python3 -m unittest discover -s generators
	python3 -m unittest discover -s scripts -p 'test_*.py'

build:
	./scripts/build.sh

deploy:
	./scripts/deploy.sh

render:
	python3 scripts/render.py --node "$${STORAGE_NODE:?Set STORAGE_NODE}"

dashboard:
	kubectl --context "$${KUBE_CONTEXT:?Set KUBE_CONTEXT}" -n context-graph port-forward service/dashboard 18088:8080
