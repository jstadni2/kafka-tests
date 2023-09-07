from os import getenv

from sqlalchemy import create_engine, URL
from sqlalchemy import Column, Integer, Identity, String
from sqlalchemy.orm import declarative_base


Base = declarative_base()


def get_db_url(host):
    return URL.create(
        "postgresql+psycopg2",
        username=getenv("POSTGRES_USER"),
        password=getenv("POSTGRES_PASSWORD"),  # plain (unescaped) text
        host=host,
        port=getenv("POSTGRES_PORT"),
        database=getenv("POSTGRES_DB"))


class Record(Base):
    __tablename__ = 'records'
    
    id = Column(Integer, Identity(start=1), primary_key=True)
    value = Column(String)
    
    def __repr__(self):
        return f"""Record(id={self.id!r},
    value={self.value!r})"""
