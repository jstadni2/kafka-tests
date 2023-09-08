import json
import logging
from os import getenv
from typing import Dict, List
from uuid import uuid4

import pytest
from confluent_kafka import Consumer, cimpl
from confluent_kafka.admin import AdminClient, NewTopic  # noqa
from sqlalchemy_utils import database_exists

from dzk.producer import produce, message_value_deserializer, transform_message_value
from dzk.models import get_db_url, Record


logger = logging.getLogger()

LOCAL_PORT = getenv("LOCAL_PORT")
BROKER_CONF = {"bootstrap.servers": f"0.0.0.0:{LOCAL_PORT}"}
CONSUMER_CONF = {
    "bootstrap.servers": f"0.0.0.0:{LOCAL_PORT}",
    "group.id": "demo-group",
    "session.timeout.ms": 6000,
    "auto.offset.reset": "earliest",
}
NUMBER_OF_MESSAGES = 20


@pytest.mark.component
def test_topic_exists(kafka_admin_client: AdminClient, new_topic: NewTopic):
    cluster_metadata = kafka_admin_client.list_topics()
    topics = cluster_metadata.topics
    assert new_topic.topic in topics.keys()


@pytest.mark.integration
@pytest.mark.parametrize(
    "produced_message", [{"id": str(uuid4())} for _ in range(NUMBER_OF_MESSAGES)]
)
def test_produce_consume(produced_message: Dict[str, str], new_topic: NewTopic):
    produced_message_bytes = json.dumps(produced_message).encode("UTF-8")
    produce(
        broker_conf=BROKER_CONF,
        topic=new_topic.topic,
        message=produced_message_bytes,
    )

    consumer = Consumer(CONSUMER_CONF)

    def on_assignment_print(
        _consumer: cimpl.Consumer, _partitions: List[cimpl.TopicPartition]
    ):
        logger.info(f"Assignment: {_partitions}")

    consumer.subscribe(topics=[new_topic.topic], on_assign=on_assignment_print)
    consumed_message = consumer.poll()
    assert consumed_message.value() == produced_message_bytes
    consumer.close()


@pytest.mark.component
def test_postgres_exists(db_connection):
    DB_HOST = getenv("DB_HOST")
    url = get_db_url(DB_HOST)
    assert database_exists(url)


# Integration test
# Expected output from db should be transformed value
@pytest.mark.parametrize(
    "produced_message", [{"id": str(uuid4())} for _ in range(1)]
)
def test_kafka_message_to_db(produced_message, new_topic, db_session):
    produced_message_bytes = json.dumps(produced_message).encode("UTF-8")
    produce(
        broker_conf=BROKER_CONF,
        topic=new_topic.topic,
        message=produced_message_bytes,
    )

    consumer = Consumer(CONSUMER_CONF)

    def on_assignment_print(
        _consumer: cimpl.Consumer, _partitions: List[cimpl.TopicPartition]
    ):
        logger.info(f"Assignment: {_partitions}")

    consumer.subscribe(topics=[new_topic.topic], on_assign=on_assignment_print)
    consumed_message = consumer.poll()
    consumer.close()
    
    # Deserialize and transform message value
    message_value = message_value_deserializer(consumed_message.value())
    transformed_value = transform_message_value(message_value)
    record = Record(**transformed_value)
    
    # Insert consumed message into db
    
    db_session.add(record)
    db_session.flush()
    db_session.commit()
    
    # Fetch database record by id
    fetched_record = db_session.get(Record, record.id)
    
    # Compared fetched record to expected transformed value
    assert fetched_record.value == transformed_value['value']
