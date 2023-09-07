CONTAINERS := $(shell docker ps -a -q)

# All targets are phony
.PHONY: *

test-component:
	pytest -m component tests/ --fixture_scope=session

test-integration:
	pytest -m integration tests/ --fixture_scope=session

clean:
	rm -rf .pytest_cache

containers:
	@echo $(CONTAINERS)

# This would be safer if it cleaned specific containers and images
clean-docker:
	docker stop $(CONTAINERS)
	docker system prune -f

# Pulling these images actually got the test suites to work
# TODO: Get docker-compose to work in lieu of these targets 

pull-zk:
	docker pull confluentinc/cp-zookeeper:6.2.0

pull-server:
	docker pull confluentinc/cp-server:6.2.0   

# Docker image troubleshooting

postgres-run:
	docker run --name some-postgres -e POSTGRES_PASSWORD=mysecretpassword -d postgres:15.4

postgres-run:
	docker run --name some-postgres -e POSTGRES_PASSWORD=mysecretpassword -d postgres:15.4 

postgres-psql:
	docker run -it --rm --network some-network postgres psql -h some-postgres -U postgres:15.4