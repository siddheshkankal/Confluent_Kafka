import argparse
import os.path
import csv
from confluent_kafka import Consumer
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.schema_registry.json_schema import JSONDeserializer
from confluent_kafka.schema_registry import SchemaRegistryClient


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

# *************** function to write data export to abn excel file which ever data subscribed by consumer ***************
def export_data_excel(header,data,path):
    file_exists = os.path.isfile(path)

    with open(path,'a', newline = '') as f:
        w = csv.DictWriter(f,fieldnames= header)
        if not file_exists:
            w.writeheader()
        w.writerow(data)
        
def main(topic):
# info -->******************  hardcoded schema ******************

#     schema_str = """
# {
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

    schema_registry_conf = schema_config()
    schema_registry_client = SchemaRegistryClient(schema_registry_conf)

# Info --> ****************** approch 1 to get schema from schema registry using subject ******************
    # subjects = schema_registry_client.get_subjects()
    # for sub in subjects:
    #     if sub == 'restaurant-take-away-data-value':
    #         schema = schema_registry_client.get_latest_version(sub)
    #         schema_str = schema.schema.schema_str

# Info --> ****************** approch 2 to get schema from schema registry using schema id ******************

    schema_str = schema_registry_client.get_schema(schema_id = 100003).schema_str       
    
    json_deserializer = JSONDeserializer(schema_str,
                                         from_dict=Restaurant.dict_to_restaurant)

    consumer_conf = sasl_conf()
    consumer_conf.update({
                     'group.id': 'group1',
                     'auto.offset.reset': "earliest"})

    consumer = Consumer(consumer_conf)
    consumer.subscribe([topic])

    counter = 0
    path = './output.csv'
    while True:
        try:
            # SIGINT can't be handled when polling, limit timeout to 1 second.
            msg = consumer.poll(1.0)
            if msg is None:
                continue

            restaurant = json_deserializer(msg.value(), SerializationContext(msg.topic(), MessageField.VALUE))

            if restaurant is not None:
                print("User record {}: restaurant: {}\n"
                      .format(msg.key(), restaurant))
                header = list(restaurant.record.keys())
                data = (restaurant.record)
                export_data_excel(header,data,path)
                
                counter+= 1
            print(f'totoal number of recorded subscribed are : {counter}')

        except KeyboardInterrupt:
            break

    consumer.close()

main("restaurant-take-away-data")