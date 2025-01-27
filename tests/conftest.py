import logging
from os import getenv
from time import sleep
from uuid import uuid4

import docker
import pytest
from confluent_kafka.admin import AdminClient, NewTopic  # noqa
from docker.client import DockerClient
from docker.models.containers import Container
from docker.models.networks import Network
from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session, sessionmaker

from dzk.models import get_db_url, Base


logger = logging.getLogger()

KAFKA_IMAGE_NAME = getenv("KAFKA_IMAGE_NAME")
ZOOKEEPER_IMAGE_NAME = getenv("ZOOKEEPER_IMAGE_NAME")
ZOOKEEPER_CLIENT_PORT = getenv("ZOOKEEPER_CLIENT_PORT")
BROKER_PORT = getenv("BROKER_PORT")
LOCAL_PORT = getenv("LOCAL_PORT")

POSTGRES_IMAGE_NAME = getenv("POSTGRES_IMAGE_NAME")
POSTGRES_PORT = getenv("POSTGRES_PORT")
POSTGRES_USER = getenv("POSTGRES_USER")
POSTGRES_PASSWORD = getenv("POSTGRES_PASSWORD")
POSTGRES_DB = getenv("POSTGRES_DB")
PGDATA = getenv("PGDATA")
DB_HOST = getenv("DB_HOST")


def pytest_addoption(parser):
    parser.addoption("--fixture_scope")


def determine_scope(fixture_name, config):
    fixture_scope = config.getoption("--fixture_scope")
    if fixture_scope is None:
        fixture_scope = "session"
    if fixture_scope in [
        "function",
        "class",
        "module",
        "package",
        "session",
    ]:
        return fixture_scope
    else:
        raise ValueError(
            "Usage: pytest tests/ --fixture_scope=function|class|module|package|session"
        )


@pytest.fixture(scope=determine_scope)
def resource_postfix() -> str:
    return str(uuid4()).partition("-")[0]


@pytest.fixture(scope=determine_scope)
def topic_name(resource_postfix: str) -> str:
    return f"demo-topic-{resource_postfix}"


@pytest.fixture(scope=determine_scope)
def consumer_id(resource_postfix: str) -> str:
    return f"demo-consumer-{resource_postfix}"


@pytest.fixture(scope=determine_scope)
def docker_client() -> DockerClient:
    return docker.from_env()


@pytest.fixture(scope=determine_scope)
def network(docker_client: DockerClient, resource_postfix: str) -> Network:
    _network = docker_client.networks.create(name=f"network-{resource_postfix}")
    yield _network
    _network.remove()


@pytest.fixture(scope=determine_scope)
def zookeeper(
    docker_client: DockerClient, network: Network, resource_postfix: str
) -> Container:
    logging.info(f"Pulling {ZOOKEEPER_IMAGE_NAME}")
    docker_client.images.get(name=ZOOKEEPER_IMAGE_NAME)
    logging.info(f"Starting container zookeeper-{resource_postfix}")

    zookeeper_container = docker_client.containers.run(
        image=ZOOKEEPER_IMAGE_NAME,
        ports={f"{ZOOKEEPER_CLIENT_PORT}/tcp": f"{ZOOKEEPER_CLIENT_PORT}/tcp"},
        network=network.name,
        name=f"zookeeper-{resource_postfix}",
        hostname="zookeeper",
        environment={"ZOOKEEPER_CLIENT_PORT": ZOOKEEPER_CLIENT_PORT},
        detach=True,
    )
    logging.info(f"Container zookeeper-{resource_postfix} started")
    yield zookeeper_container
    zookeeper_container.remove(force=True)


@pytest.fixture(scope=determine_scope)
def broker(
    docker_client: DockerClient,
    network: Network,
    zookeeper: Container,
    resource_postfix: str,
) -> Container:
    logging.info(f"Pulling {KAFKA_IMAGE_NAME}")
    docker_client.images.get(name=KAFKA_IMAGE_NAME)
    logging.info(f"Starting container broker-{resource_postfix}")
    broker_container = docker_client.containers.run(
        image=KAFKA_IMAGE_NAME,
        ports={
            f"{BROKER_PORT}/tcp": f"{BROKER_PORT}/tcp",
            f"{LOCAL_PORT}/tcp": f"{LOCAL_PORT}/tcp",
        },
        network=network.name,
        name=f"broker-{resource_postfix}",
        hostname="broker",
        environment={
            "KAFKA_BROKER_ID": 25,
            "KAFKA_ZOOKEEPER_CONNECT": f"{zookeeper.name}:{ZOOKEEPER_CLIENT_PORT}",
            "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP": "PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT",
            "KAFKA_ADVERTISED_LISTENERS": f"PLAINTEXT://broker:{BROKER_PORT},PLAINTEXT_HOST://localhost:{LOCAL_PORT}",
            "KAFKA_METRIC_REPORTERS": "io.confluent.metrics.reporter.ConfluentMetricsReporter",
            "KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR": 1,
            "KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS": 0,
            "KAFKA_CONFLUENT_LICENSE_TOPIC_REPLICATION_FACTOR": 1,
            "KAFKA_CONFLUENT_BALANCER_TOPIC_REPLICATION_FACTOR": 1,
            "KAFKA_TRANSACTION_STATE_LOG_MIN_ISR": 1,
            "KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR": 1,
            "KAFKA_JMX_PORT": 9101,
            "KAFKA_JMX_HOSTNAME": "localhost",
            "CONFLUENT_METRICS_REPORTER_BOOTSTRAP_SERVERS": f"broker:{BROKER_PORT}",
            "CONFLUENT_METRICS_REPORTER_TOPIC_REPLICAS": 1,
            "CONFLUENT_METRICS_ENABLE": "true",
            "CONFLUENT_SUPPORT_CUSTOMER_ID": "anonymous",
        },
        detach=True,
    )
    logging.info(f"Container broker-{resource_postfix} started")
    yield broker_container
    broker_container.remove(force=True)


@pytest.fixture(scope=determine_scope)
def kafka_admin_client(broker: Container) -> AdminClient:
    has_started = False
    kafka_logs = set()
    while not has_started:
        log_line = broker.logs(tail=1).decode("UTF-8").strip()
        if log_line in kafka_logs:
            pass
        else:
            kafka_logs.add(log_line)
            logger.info(log_line)
        if "INFO Kafka startTimeMs" in log_line:
            has_started = True
            logging.info("Kafka has started")

    admin_client = AdminClient(conf={"bootstrap.servers": f"0.0.0.0:{LOCAL_PORT}"})

    return admin_client


@pytest.fixture(scope=determine_scope)
def new_topic(kafka_admin_client: AdminClient, topic_name: str) -> NewTopic:
    new_topic = NewTopic(
        topic=topic_name,
        num_partitions=1,
        replication_factor=1,
    )
    kafka_admin_client.create_topics(new_topics=[new_topic])
    topic_exists = False
    while not topic_exists:
        logging.info(f"Waiting for topic {new_topic.topic} to be created")
        sleep(1)
        cluster_metadata = kafka_admin_client.list_topics()
        topics = cluster_metadata.topics
        topic_exists = new_topic.topic in topics.keys()
    yield new_topic
    kafka_admin_client.delete_topics(topics=[new_topic.topic])


@pytest.fixture(scope=determine_scope)
def postgres_service_name(resource_postfix: str):
    return f"postgres-{resource_postfix}"


@pytest.fixture(scope=determine_scope)
def postgres(
    docker_client: DockerClient,
    network: Network,
    postgres_service_name: str
    )-> Container:
    logging.info(f"Pulling {POSTGRES_IMAGE_NAME}")
    docker_client.images.get(name=POSTGRES_IMAGE_NAME)
    logging.info(f"Starting container {postgres_service_name}")

    postgres_container = docker_client.containers.run(
        image=POSTGRES_IMAGE_NAME,
        ports={f"{POSTGRES_PORT}": f"{POSTGRES_PORT}"},
        network=network.name,
        name=postgres_service_name,
        hostname=postgres_service_name,  # use this?
        environment={"POSTGRES_PORT": POSTGRES_PORT,
                     "POSTGRES_USER": POSTGRES_USER,
                     "POSTGRES_PASSWORD": POSTGRES_PASSWORD,
                     "POSTGRES_DB": POSTGRES_DB,
                    # "PGDATA": "/var/lib/postgresql/data/pgdata",
                    },
        detach=True,
    )
    sleep(1)
    logging.info(f"Container {postgres_service_name} started")
    
    yield postgres_container
    postgres_container.remove(force=True)


@pytest.fixture(scope="function")
def db_connection(postgres, postgres_service_name):
    """SQLAlchemy connection for an empty database.

    Yields:
        _type_: _description_
    """
    # TODO: successfully call get_db_url on network/container
    test_engine = create_engine(get_db_url(DB_HOST))
    Base.metadata.create_all(test_engine)
    connection = test_engine.connect()
    yield connection
    connection.close()
    Base.metadata.drop_all(bind=test_engine)


@pytest.fixture(scope="function")
def db_session(postgres, postgres_service_name):
    """SQLAlchemy connection for an empty database.

    Yields:
        _type_: _description_
    """
    # TODO: successfully call get_db_url on network/container
    test_engine = create_engine(get_db_url(DB_HOST))
    Base.metadata.create_all(test_engine)
    Session = scoped_session(sessionmaker(bind=test_engine))
    session = Session()
    yield session
    session.close()
    Base.metadata.drop_all(bind=test_engine)
