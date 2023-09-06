from os import getenv

from sqlalchemy import create_engine
from sqlalchemy import Column, Integer, Identity, String
from sqlalchemy.orm import declarative_base


Base = declarative_base()


# TODO: Refactor this for pytest.ini/Docker config
def get_db_url(instance):
    db_instances = {'test': getenv("DATABASE_URL_TEST"),
                    'prod': getenv("DATABASE_URL")}

    if instance not in db_instances:
        raise ValueError("Invalid database instance. Enter either 'test' or 'prod'")

    url = db_instances[instance]

    return url


# TODO: Refactor this for pytest.ini config
def get_db_engine(instance):
    return create_engine(get_db_url(instance))


class Record(Base):
    __tablename__ = 'records'
    
    id = Column(Integer, Identity(start=1), primary_key=True)
    value = Column(String)
    
    def __repr__(self):
        return f"""Record(id={self.id!r},
    value={self.value!r})"""
