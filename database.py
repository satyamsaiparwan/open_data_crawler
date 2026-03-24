from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime
from sqlalchemy.orm import scoped_session, sessionmaker, declarative_base
import datetime

engine = create_engine('sqlite:///government_data.db')
db_session = scoped_session(sessionmaker(autocommit=False, autoflush=False, bind=engine))
Base = declarative_base()
Base.query = db_session.query_property()

class GovernmentData(Base):
    __tablename__ = 'government_data'
    id = Column(Integer, primary_key=True)
    category = Column(String(100), nullable=False)
    identifier = Column(String(100), nullable=False)
    value = Column(Float, nullable=False)
    timestamp = Column(DateTime, default=datetime.datetime.utcnow)

    def to_dict(self):
        return {
            'id': self.id,
            'category': self.category,
            'identifier': self.identifier,
            'value': self.value,
            'timestamp': self.timestamp.strftime("%Y-%m-%d %H:%M:%S")
        }

def init_db():
    Base.metadata.create_all(bind=engine)
