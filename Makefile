# Running tests from the command line fails, but running
# them from VS Code passes
test-component:
	pytest -m component tests/ --fixture_scope=session

test-integration:
	pytest -m integration tests/ --fixture_scope=session

clean:
	rm -rf .pytest_cache

# Pulling these images actually got the test suites to work

pull-zk:
	docker pull confluentinc/cp-zookeeper:6.2.0

pull-server:
	docker pull confluentinc/cp-server:6.2.0   