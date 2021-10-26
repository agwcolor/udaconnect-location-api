from app.udaconnect.models import Location
from app.udaconnect.schemas import LocationSchema
from app.udaconnect.services import LocationService
from flask import request
from flask_accepts import accepts, responds
from flask_restx import Namespace, Resource
from typing import Optional, List

from app import g
# from kafka import KafkaConsumer


DATE_FORMAT = "%Y-%m-%d"

api = Namespace("UdaConnect", description="Connections via geolocation.")  # noqa


# TODO: This needs better exception handling

# refactor to separate api using kafka
# separate get and put for location
#
# ====================
# POST with parameters
# ====================
'''
@api.route("/locations")
@api.param("creation_time", "Creation Time", _in="query")
@api.param("latitude", "Latitude", _in="query")
@api.param("longitude", "Longitude", _in="query")
@api.param("person_id", "Unique ID for a given Person", _in="query")
@api.param("id", "Location id", _in="query")

class LocationResource(Resource):
    print("hello")
    @accepts(schema=LocationSchema)
    @responds(schema=LocationSchema)
    def post(self) -> Location:
        data = request.get_json()
        # print(data, " is the data")
        new_location: Location = LocationService.create(data)
        print(new_location, "is the location")
        return new_location
'''
    # ====================
    # POST without parameters - stub
    # ====================
@api.route("/locations")
class LocationResource(Resource):
    @responds(schema=LocationSchema)
    def post(self) -> Location:
        data = {
                "id": 29,
                "person_id": 5,
                "longitude":"37.553441",
                "latitude":"-122.290524",
                "creation_time":"2020-08-18T10:37:0"
                }
        # print(data, " is the data")
        new_location: Location = LocationService.create(data)
        print(new_location, "is the location")
        return new_location
    

    @responds(schema=LocationSchema, many=True)
    def get(self) -> List[Location]:
        locations: List[Location] = LocationService.retrieve_all()
        return locations

    @api.route("/locations/<location_id>")
    class LocationResource(Resource):
        @responds(schema=LocationSchema)
        def get(self, location_id) -> Location:
           location: Location = LocationService.retrieve(location_id)
           return location


# Troubleshooting Notes
'''
# Unable to get post to work with decorators
# Invalid schema error
@accepts(schema=LocationSchema)
@responds(schema=LocationSchema)

def post(self) -> Location:
    payload = request.get_json()
    print(type(payload))
    kafka_data = payload.encode()
    kafka_producer = g.kafka_producer
    kafka_producer.send("connections", kafka_data)

    consumer = KafkaConsumer("connections")
    for message in consumer:
        print (message)
        new_location: Location = LocationService.create(payload)
    return new_location
'''