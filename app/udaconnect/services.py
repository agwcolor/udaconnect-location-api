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

        # set up kafka consumer to put into database
        consumer = KafkaConsumer(
            'connections',
            bootstrap_servers=['localhost:9092'],
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            )
        for message in consumer:
            print(type(message), "Is the message type")
            message_value = message.value
            message_converted = json.loads(message_value.decode('utf-8'))
            print("This is the message converted", message_converted)
            new_location = Location()
            new_location.person_id = location["person_id"]
            new_location.creation_time = location["creation_time"]
            new_location.coordinate = ST_Point(location["latitude"], location["longitude"])
            print(new_location, "is the location transmitted by kafka")
            # db.session.add(new_location)
            # db.session.commit()
            # print("I'm in the consumer and here's the message!", message)

        # return location
        print(message, " is the message!")
        return message


