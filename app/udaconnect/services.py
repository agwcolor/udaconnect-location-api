import logging
from typing import Dict, List

from app import db
from app.udaconnect.models import Location
from app.udaconnect.schemas import LocationSchema
from geoalchemy2.functions import ST_AsText, ST_Point
from sqlalchemy.sql import text
import json
from app import g
from kafka import KafkaConsumer


logging.basicConfig(level=logging.WARNING)
logger = logging.getLogger("udaconnect-location-api")

class LocationService:
    @staticmethod
    def retrieve(location_id) -> Location:
        location, coord_text = (
            db.session.query(Location, Location.coordinate.ST_AsText())
            .filter(Location.id == location_id)
            .one()
        )

        # Rely on database to return text form of point to reduce overhead of conversion in app code
        location.wkt_shape = coord_text
        return location

    @staticmethod
    def retrieve_all() -> List[Location]:
        return db.session.query(Location).all()

    @staticmethod
    def create(location: Dict) -> Location:
        print(type(location), location, " is the location in the create method")
        validation_results: Dict = LocationSchema().validate(location)
        if validation_results:
            logger.warning(f"Unexpected data format in payload: {validation_results}")
            raise Exception(f"Invalid payload: {validation_results}")

        # set up kafka producer for new location data
        kafka_data = location
        print(kafka_data, " is the kafka data")
        kafka_producer = g.kafka_producer
        kafka_producer.send("connections", value=kafka_data)
        kafka_producer.flush()

        # set up kafka consumer to put into database
        consumer = KafkaConsumer("connections")
        for message in consumer:
            new_location = Location()
            new_location.person_id = location["person_id"]
            new_location.creation_time = location["creation_time"]
            new_location.coordinate = ST_Point(location["latitude"], location["longitude"])
            print(new_location, "is the object")
            # db.session.add(new_location)
            # db.session.commit()
            print("I'm in the consumer and here's the message!", message)

        return location


