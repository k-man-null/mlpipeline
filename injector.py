import random
import requests
import json
from paho.mqtt import client as mqtt_client
import time


url = "http://uoweb3.ncl.ac.uk/api/v1.1/sensors/PER_AIRMON_MONITOR1135100/data/json/?starttime=20220601&endtime=20220831"
    
    #Request data from Urban Repository Platform

response = requests.get(url=url)

    #convert response(JSON) to dictionaty format

raw_data_dict = response.json()



sensors = raw_data_dict["sensors"]



# for data in sensors:

#     for reading in data["data"]["PM2.5"]:

    
#         timestamp = reading["Timestamp"]
#         value = reading["Value"]
#     #message = bytearray(dict(timestamp = timestamp, value = value)) 

#         message = f"Timestamp: {timestamp}, Value: {value}"
#         print(message)




def on_connect(client, userdata, flags, rc):

    print("Called")
    if rc == 0:
        print("Connected to MQTT Broker!", flush=True)
    else:
        print("Failed to connect, return code %d\n", rc, flush=True)

def publish(client, topic, sensors):
    msg_count = 0

    for reading in sensors[0]["data"]["PM2.5"]:
        time.sleep(0.5)

        timestamp = reading["Timestamp"]
        value = reading["Value"]
    

        message = f"Message {msg_count} Timestamp: {timestamp}, Value: {value}"
        print(message)
        result = client.publish(topic=topic,payload=message,qos=2, retain=True)


        status = result[0]
        if status == 0:
            print(f"Send `{message}` to topic `{topic}`")
        else:
            print(f"Failed to send message to topic {topic}")
        msg_count += 1
    print(msg_count)


    
def run(sensors):
    mqtt_ip = "172.17.0.2"
    mqtt_port = 1883
    topic = "NO2inair"
    

    client_id = f'python-mqtt-{random.randint(0, 1000)}'

    client = mqtt_client.Client(client_id,clean_session=False)

    client.on_connect = on_connect

    client.connect(mqtt_ip, mqtt_port)

    client.loop_start()
    publish(client, topic=topic, sensors=sensors)

if __name__ == '__main__':
    run(sensors)
    
    