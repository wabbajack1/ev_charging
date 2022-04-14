#!/usr/bin/env python3

from sqlalchemy import *
import sqlalchemy.orm as orm
from sqlalchemy.orm import relationship


mapper_registry = orm.registry()

Base = mapper_registry.generate_base()


DB_CONFIG_DICT = {
        'user': 'KerimErekmen',
        'password': '',
        'host': 'localhost',
        'port': 5432,
        }

DB_CONN_FORMAT = "postgresql://{user}:{password}@{host}:{port}/{database}"

DB_CONN_URI_DEFAULT = (DB_CONN_FORMAT.format(
        database='utilization_ev_stations',
        **DB_CONFIG_DICT))

engine = create_engine(DB_CONN_URI_DEFAULT, echo=True)

# stations (Dimension Table)
class Stations(Base):
    __tablename__ = 'stations'
    id = Column(String, primary_key=True) # station id
    country_code = Column(String) # for boolean operation in query
    name = Column(String) # Name of a charging station
    address = Column(String)
    city = Column(String)
    postal_code = Column(String)
    coordinates = Column(String)
    parking_type = Column(String)
    speed = Column(String)
    status = Column(String) # status of station

    parent = relationship("EVSES", back_populates="stations_object")


    def __repr__(self):
       pass
       #return f"User(id={self.id!r}, name={self.name!r}, fullname={self.fullname!r})

# operator of station (Dimension Table)
class Operator(Base):
    __tablename__ = 'operator'
    id = Column(String, primary_key=True)
    name = Column(String) # name of company

    parent = relationship("EVSES", back_populates="operator_object")

    
    def __repr__(self):
      return "Operator(id={}, name={})".format(self.id, self.name)

 # connectors of each EVSES (Dimension Table) (List of available connectors on an EVSE)
class Connectors(Base):
    __tablename__ = 'connectors'
    id = Column(String, primary_key=True)
    standard = Column(String)
    format_connector = Column(String)
    power_type = Column(String) # eg. AC_3_PHASE
    power = Column(Integer) # 11 kW

    parent = relationship("EVSES", back_populates="connectors_object")

# Timestamp when a EVSEs or Connectors were last updated (or created) (Dimension Table)
class Date_(Base):
    __tablename__ = 'date'

    id = Column(String, primary_key=True)
    last_updated = Column(String)

    parent = relationship("EVSES", back_populates="date_object")
      
class EVSES(Base):
    """ Faktentabelle
    Hier sind die Einzelnen EVSES (Ladesäulen einer EV-Station)
    Spalten: id, Dimensionskeys, status (Indicates the current status of an EVSE)
    Args:
        Base (_type_): _description_

    Returns:
        _type_: _description_
    """
    __tablename__ = 'evses'
    id = Column(Integer, primary_key=True, autoincrement=True) # for each event (event == charging status)
    evses_id = Column(String) # uid with eMI3 standard
    station_id = Column(String, ForeignKey('stations.id'))
    operator_id = Column(String, ForeignKey('operator.id'))
    connectors_id = Column(String, ForeignKey('connectors.id'))   
    date_id = Column(String, ForeignKey('date.id'))

    status_evses = Column(String) # Indicates the current status of an EVSE; fact data for utilization anlyses
    aufruf = Column(String) # Time is String because with datetime module we can operate on it; now = datetime.now(); now.strftime("%d/%m/%Y, %H:%M:%S") -> to string

    operator_object = relationship("Operator", back_populates="parent")
    stations_object = relationship("Stations", back_populates="parent")
    connectors_object = relationship("Connectors", back_populates="parent")
    date_object = relationship("Date_", back_populates="parent")
    
    
    #def __repr__(self):
     #   return f"Address(id={self.id!r}, email_address={self.email_address!r})"


# if call "Query stations around" == DE then use "id" and call "Query station detail" else discard

if __name__ == "__main__":

    Base.metadata.create_all(engine)