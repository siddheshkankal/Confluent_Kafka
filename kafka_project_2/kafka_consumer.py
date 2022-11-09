import argparse
import os.path
import csv
from confluent_kafka import Consumer
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.schema_registry.json_schema import JSONDeserializer
from confluent_kafka.schema_registry import SchemaRegistryClient


API_KEY = 'UWNWP4ER6EPLO356'
ENDPOINT_SCHEMA_URL  = 'https://psrc-mw2k1.us-east-2.aws.confluent.cloud'
API_SECRET_KEY = 'A1G/Q/R0NK9Tyo+PC8xfmngQuZHn/FJM+XHZW1NBOhE3NUgNf8QITq3h15pO6ThE'
BOOTSTRAP_SERVER = 'pkc-ymrq7.us-east-2.aws.confluent.cloud:9092'
SECURITY_PROTOCOL = 'SASL_SSL'
SSL_MACHENISM = 'PLAIN'
SCHEMA_REGISTRY_API_KEY = 'UJ3LZ3TPAF3IGBTC'
SCHEMA_REGISTRY_API_SECRET = 'qfY+TKn+k7cjxcMEzkWMvz67B1a7pSnaePFMQBwsOcJiPhjR5nwniN+aVg4I4FAZ' 


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

class Covid:   
    def __init__(self,record:dict):
        for k,v in record.items():
            setattr(self,k,v)
        
        self.record=record
   
    @staticmethod
    def dict_to_covid(data:dict,ctx):
        return Covid(record=data)

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

    schema_registry_conf = schema_config()
    schema_registry_client = SchemaRegistryClient(schema_registry_conf)


    schema_str = schema_registry_client.get_schema(schema_id = 100003).schema_str       
    
    json_deserializer = JSONDeserializer(schema_str,
                                         from_dict=Covid.dict_to_covid)

    consumer_conf = sasl_conf()
    consumer_conf.update({
                     'group.id': 'group1',
                     'auto.offset.reset': "earliest"})

    consumer = Consumer(consumer_conf)
    consumer.subscribe([topic])

    counter = 0
    # path = './output.csv'
    while True:
        try:
            # SIGINT can't be handled when polling, limit timeout to 1 second.
            msg = consumer.poll(1.0)
            if msg is None:
                continue

            covid = json_deserializer(msg.value(), SerializationContext(msg.topic(), MessageField.VALUE))
            # print(msg.value())
            # print(covid.record['State/UnionTerritory'])
            if covid is not None:
                print("User record {}: covid: {}\n"
                      .format(msg.key(), covid))
                header = list(covid.record.keys())
                data = (covid.record)
                if covid.record['State/UnionTerritory'] == 'Delhi':
                    export_data_excel(header,data,'./delhi_covid_data.csv')
                elif covid.record['State/UnionTerritory'] == 'Karnataka':
                    export_data_excel(header,data,'./Karnataka_covid_data.csv')
                elif covid.record['State/UnionTerritory'] == 'Maharashtra':
                    export_data_excel(header,data,'./Maharashtra_covid_data.csv')
                elif covid.record['State/UnionTerritory'] == 'Madhya Pradesh':
                    export_data_excel(header,data,'./Madhya_Pradesh_covid_data.csv')
                else:
                    export_data_excel(header,data,'./restAll_covid_data.csv')




                counter+= 1
            print(f'totoal number of recorded subscribed are : {counter}')

        except KeyboardInterrupt:
            break

    consumer.close()

main("covid_19_india")