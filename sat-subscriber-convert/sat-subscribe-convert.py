import os
import requests
import boto3
import csv
import ssl
import logging
import random
import paho.mqtt.client as mqtt
import re
import json
import sys

from epct import api

MQTT_USERNAME = os.getenv("MQTT_USERNAME")
MQTT_PWD = os.getenv("MQTT_PWD")
MQTT_HOST = os.getenv("MQTT_HOST")
MQTT_PORT = os.getenv("MQTT_PORT", 8883)

S3_KEY_ID = os.getenv("S3_KEY_ID")
S3_SECRET_KEY = os.getenv("S3_SECRET_KEY")
S3_BUCKET = os.getenv("S3_BUCKET")

ORIGIN_USERNAME = os.getenv("ORIGIN_USERNAME")
ORIGIN_PASSWORD = os.getenv("ORIGIN_PASSWORD")

DATA_MAPPING = {}

LOG_LEVEL = os.getenv("LOG_LEVEL", "WARNING")
LOGGER = logging.getLogger('wis2-sat-subscriber')
LOGGER.setLevel(LOG_LEVEL)
logging.basicConfig(stream=sys.stdout)

def upload_file_to_s3(local_file,object_key):
    session = boto3.Session(
        aws_access_key_id=S3_KEY_ID,
        aws_secret_access_key=S3_SECRET_KEY
    )
    s3client = session.client('s3')
    print(f"upload file with key={object_key}")
    with open(local_file,"rb") as f:
        s3client.upload_fileobj(f,S3_BUCKET,object_key)


def convert_file(input_file,output_format):
    chain_config = {'product': 'HRSEVIRI_HRIT', 'format': output_format}
    output_files = api.run_chain(
        [input_file],
        chain_config=chain_config,
        target_dir='/data/'
    )
    if output_files is None:
        print("no output produced!")
        return
    for output_file in output_files:
        print(f"output-file: {output_file}")
        prefix = 'wis2-sat-data/'
        filename = input_file.split('/')[-1]
        object_key = f"{filename.split('.')[0]}.{output_format}"
        upload_file_to_s3(output_file,'wis2-sat-data/'+object_key)
        os.remove(output_file)

def sub_connect(client, userdata, flags, rc, properties=None):
    LOGGER.info(f"on connection to subscribe: {mqtt.connack_string(rc)}")
    for topic in DATA_MAPPING:
        LOGGER.info(f'subscribing to topic: {topic}')
        client.subscribe(topic, qos=1)

def sub_on_message(client, userdata, msg):
    """
      do something with the message
      in this case, convert to geotiff and netcdf
    """
    try:
        message = json.loads(msg.payload.decode('utf-8'))
        if "links" in message:
            data_url = message["links"][0]["href"]
            data_type = message["links"][0]["type"]
            LOGGER.info(f"data_url={data_url}")
            LOGGER.info(f"data_type={data_type}")
            try:
                print(f"data_url: {data_url}")
                if "HRV" in str(data_url):
                    print("skip data_url containing HRV")
                    return
                resp = requests.get(data_url,auth=(ORIGIN_USERNAME,ORIGIN_PASSWORD))
                resp.raise_for_status()
                file_name = "/tmp/"+data_url.split('/')[-1]
                print(f"write response to {file_name}")
                with open(os.path.join(data_url,file_name), 'wb') as fd:
                    fd.write(resp.content)
                if "PRO" in str(data_url):
                    print("don't convert prologue file")
                    return
                # convert to geotiff
                print(f"convert to geotiff")
                convert_file(file_name,'geotiff')
                os.remove(file_name)
            except Exception as e:
                LOGGER.warning(f"Error {e}")
                return
    except Exception as e:
        LOGGER.error(f'Failed: {e}')


def run_wis2_subscriber():
    r = random.Random()
    client_id = f"{__name__}_{r.randint(1,1000):04d}"
    client = mqtt.Client(client_id=client_id, protocol=mqtt.MQTTv5)
    if MQTT_PORT == 8883:
        client.tls_set(
            certfile=None,
            keyfile=None,
            cert_reqs=ssl.CERT_REQUIRED
        )
    client.username_pw_set(MQTT_USERNAME, MQTT_PWD)
    client.on_connect = sub_connect
    client.on_message = sub_on_message
    client.connect(MQTT_HOST, port=int(MQTT_PORT))
    client.loop_forever()


def main():
    print(f'Starting wis2-subscriber with LOG_LEVEL={LOG_LEVEL}')
    LOGGER.info("run wis2-subscriber")
    LOGGER.info(f'MQTT_HOST={MQTT_HOST}')
    LOGGER.info(f'MQTT_USERNAME={MQTT_USERNAME}')
    LOGGER.info(f'MQTT_PORT={MQTT_PORT}')

    if not MQTT_HOST:
        LOGGER.error('MQTT_HOST is not defined')
        return
    data_mapping_file = '/app/data_mapping.csv'
    print(f"Read configuration from {data_mapping_file}: ")
    with open(data_mapping_file) as file:
        reader = csv.reader(file)
        next(reader, None)
        for data in reader:
            topic = data[0].rstrip()
            if topic == 'mqtt_topic':
                continue
            print(f' * topic={topic}')
            DATA_MAPPING[topic] = {}
    if len(DATA_MAPPING.keys()) > 0:
        print(f'Configuration provided {len(DATA_MAPPING.keys())} mqtt-topics')
        run_wis2_subscriber()
    else:
        print('0 topics defined, exiting')

if __name__ == "__main__":
    main()

