# wis2-eumetsat-subscribe-convert
subscribe to eumetsat-data on WIS2 and convert and store output in S3-bucket

input-topic is subscribed by sat-subscriber-conver/data_mappings.csv

used in WMO demo

Requires a .env file containing

```bash
MQTT_HOST=XXX # wis2-broker-host
MQTT_USERNAME=XXX # wis2-broker-username
MQTT_PWD=XX # wis2-broker-pwd

LOG_LEVEL=INFO
S3_KEY_ID='XXX'
S3_SECRET_KEY='XXX'
S3_BUCKET='mybucket'

# for encrypted data
ORIGIN_USERNAME='XXX'
ORIGIN_PASSWORD='XXX'
```