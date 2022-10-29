


# A simple example demonstrating use of JSONSerializer.

import argparse
from msilib.schema import Registry
from uuid import uuid4
from six.moves import input
from confluent_kafka import Producer
from confluent_kafka.serialization import StringSerializer, SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.json_schema import JSONSerializer
#from confluent_kafka.schema_registry import *
import pandas as pd
from typing import List

FILE_PATH = "I:/notes/kafka/Kafka Assignment/restaurant_orders.csv"
columns=['Order Number','Order Date','Item Name','Quantity','Product Price','Total products']

API_KEY = '4PPW7OGNHXA67J6E'
ENDPOINT_SCHEMA_URL  = 'https://psrc-8kz20.us-east-2.aws.confluent.cloud'
API_SECRET_KEY = 'KEqgiT9d5sZYOv1lWUQ/P0LbYz0pNfvBySJlTIEoKSliC7MiC3Xe3WogbZKk4W4N'
BOOTSTRAP_SERVER = 'pkc-ymrq7.us-east-2.aws.confluent.cloud:9092'
SECURITY_PROTOCOL = 'SASL_SSL'
SSL_MACHENISM = 'PLAIN'
SCHEMA_REGISTRY_API_KEY = 'IJ3NWQO3TZP6SHWF'
SCHEMA_REGISTRY_API_SECRET = 'dDKDAKqyVYEORxtoBB2g+fcYQ2aJGhoVluXpAj/DKP5qptI4hs2Bw+9dUDhR5LGY'


def sasl_conf():

    sasl_conf = {'sasl.mechanism': SSL_MACHENISM,
                 # Set to SASL_SSL to enable TLS support.
                #  'security.protocol': 'SASL_PLAINTEXT'}
                'bootstrap.servers':BOOTSTRAP_SERVER,
                'security.protocol': SECURITY_PROTOCOL,
                'sasl.username': API_KEY,
                'sasl.password': API_SECRET_KEY
                }
    return sasl_conf



def schema_config():
    return {'url':ENDPOINT_SCHEMA_URL,
    
    'basic.auth.user.info':f"{SCHEMA_REGISTRY_API_KEY}:{SCHEMA_REGISTRY_API_SECRET}"

    }


class Restaurant:   
    def __init__(self,record:dict):
        for k,v in record.items():
            setattr(self,k,v)
        
        self.record=record
   
    @staticmethod
    def dict_to_restaurant(data:dict,ctx):
        return Restaurant(record=data)

    def __str__(self):
        return f"{self.record}"


def get_restaurant_instance(file_path):
    df=pd.read_csv(file_path)
    df=df.iloc[:,:]
    restaurants:List[Restaurant]=[]
    for data in df.values:
        restaurant=Restaurant(dict(zip(columns,data)))
        restaurants.append(restaurant)
        yield restaurant

def restaurant_to_dict(restaurant:Restaurant, ctx):
    """
    Returns a dict representation of a User instance for serialization.
    Args:
        user (User): User instance.
        ctx (SerializationContext): Metadata pertaining to the serialization
            operation.
    Returns:
        dict: Dict populated with user attributes to be serialized.
    """

    # User._address must not be serialized; omit from dict
    return restaurant.record


def delivery_report(err, msg):
    """
    Reports the success or failure of a message delivery.
    Args:
        err (KafkaError): The error that occurred on None on success.
        msg (Message): The message that was produced or failed.
    """

    if err is not None:
        print("Delivery failed for User record {}: {}".format(msg.key(), err))
        return
    print('User record {} successfully produced to {} [{}] at offset {}'.format(
        msg.key(), msg.topic(), msg.partition(), msg.offset()))


def main(topic):
#*************************** start schema is hardcoded *************************** 
#     schema_str = """
#     {
#   "$id": "http://example.com/myURI.schema.json",
#   "$schema": "http://json-schema.org/draft-07/schema#",
#   "additionalProperties": false,
#   "description": "Sample schema to help you get started.",
#   "properties": {  					
#     "Order Number": {
#       "description": "The type(v) type is used.",
#       "type": "number"
#     },
#     "Order Date": {
#       "description": "The type(v) type is used.",
#       "type": "string"
#     },
#     "Item Name": {
#       "description": "The type(v) type is used.",
#       "type": "string"
#     },
#     "Quantity": {
#       "description": "The type(v) type is used.",
#       "type": "number"
#     },
#     "Product Price": {
#       "description": "The type(v) type is used.",
#       "type": "number"
#     },
#     "Total products": {
#       "description": "The type(v) type is used.",
#       "type": "number"
#     }
#   },
#   "title": "SampleRecord",
#   "type": "object"
# }
#     """

#*************************** end  hardcoded  schema *************************** 

    schema_registry_conf = schema_config()
    schema_registry_client = SchemaRegistryClient(schema_registry_conf)

#********************  first approact to get schema from schema Registry using subjects  ********************
    # subjects = schema_registry_client.get_subjects()
    # for sub in subjects:
    #     if sub == 'restaurant-take-away-data-value':
    #         schema = schema_registry_client.get_latest_version(sub)
    #         schema_str = schema.schema.schema_str

#********************  second  approact to get schema from schema Registry using schema_id  ********************    
    schema_str = schema_registry_client.get_schema(schema_id = 100003).schema_str


    string_serializer = StringSerializer('utf_8')
    json_serializer = JSONSerializer(schema_str, schema_registry_client, restaurant_to_dict)

    producer = Producer(sasl_conf())

    print("Producing user records to topic {}. ^C to exit.".format(topic))
    #while True:
        # Serve on_delivery callbacks from previous calls to produce()
    producer.poll(0.0)

    # info --> counter is used to know how many records published to the topic 
    counter = 0
    try:
        for restaurant in get_restaurant_instance(file_path=FILE_PATH):
        
            print(restaurant)
            producer.produce(topic=topic,
                                key=string_serializer(str(uuid4()), restaurant_to_dict),
                                value=json_serializer(restaurant, SerializationContext(topic, MessageField.VALUE)),
                                on_delivery=delivery_report)
            counter += 1

            # info --> loop is break when counter is 5 because we want to publish only 5 records as of now
            if counter == 5:
                break

        # info --> printing at the end how many records got published successfully 
        print(f'totoal number of recorded published are : {counter}')

    except KeyboardInterrupt:
        pass
    except ValueError:
        print("Invalid input, disrestaurantding record...")
        pass

    print("\nFlushing records...")
    producer.flush()

main("restaurant-take-away-data")
