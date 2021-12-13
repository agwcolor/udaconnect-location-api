from flask import Flask, jsonify, g
from flask_cors import CORS
from flask_restx import Resource, Api
from flask_sqlalchemy import SQLAlchemy
from kafka import KafkaProducer
import json


db = SQLAlchemy()


def create_app(env=None):
    from app.config import config_by_name
    from app.routes import register_routes

    app = Flask(__name__)
    app.config.from_object(config_by_name[env or "test"])
    api = Api(app, title="UdaConnect Location API", version="0.1.0")

    CORS(app)  # Set CORS for development

    register_routes(api, app)
    db.init_app(app)
    
    @app.before_request
    def before_request():
        # Set up a Kafka producer
        TOPIC_NAME = 'connections'
        KAFKA_SERVER = 'kafka-headless.default.svc.cluster.local:9092'
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_SERVER,
            api_version=(0,10,1),
            value_serializer=lambda x: json.dumps(x).encode('utf-8'))
        # Setting Kafka to g enables us to use this
        # in other parts of our application
        g.kafka_producer = producer
    

    @app.route("/health")
    def health():
        return jsonify("healthy")

    return app
