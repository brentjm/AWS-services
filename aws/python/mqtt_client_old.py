from AWSIoTPythonSDK.MQTTLib import AWSIoTMQTTClient
import logging
import time
import json
import random
import argparse
import socket
from pdb import set_trace


# Configure logging
logger = logging.getLogger("AWSIoTPythonSDK.core")
logger.setLevel(logging.DEBUG)
streamHandler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
streamHandler.setFormatter(formatter)
logger.addHandler(streamHandler)



def customCallback(client, userdata, message):
    print("Received a new message: ")
    print(message.payload)
    print("from topic: ")
    print(message.topic)
    print("--------------\n\n")

#  Device and enpoint in information file instead
#parser = argparse.ArgumentParser()
#parser.add_argument("device_name")
#parser.add_argument("host", help="The AWS IoT Endpoint")
#args = parser.parse_args()
#device = args.device_name

with open("connection_info.json") as fObj:
    info = json.loads(fObj.read())
CLIENT_ID = info["device_name"]
HOST = info["endpoint"]

# Constants
#CLIENT_ID = args.device_name
ROOT_CA_PATH = "root-CA.crt"
PRIVATE_KEY_PATH = "{}.private.key".format(CLIENT_ID)
CERT_PATH = "{}.cert.pem".format(CLIENT_ID)
#HOST = args.host
PORT = 8883
TOPIC = "Chillers/{}/status".format(CLIENT_ID)
IP = ""
PORT = 5007

# Socket
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.bind((IP, PORT))
s.listen(1)
conn, addr = s.accept()
print("Connected by: {}".format(addr))

# Init AWSIoTMQTTClient
myAWSIoTMQTTClient = AWSIoTMQTTClient(CLIENT_ID)
myAWSIoTMQTTClient.configureEndpoint(HOST, PORT)
myAWSIoTMQTTClient.configureCredentials(ROOT_CA_PATH, PRIVATE_KEY_PATH, CERT_PATH)

# AWSIoTMQTTClient connection configuration
myAWSIoTMQTTClient.configureAutoReconnectBackoffTime(1, 32, 20)
# myAWSIoTMQTTClient.configureOfflinePublishQueueing(-1)  # Infinite offline Publish queueing
myAWSIoTMQTTClient.configureDrainingFrequency(2)  # Draining: 2 Hz
myAWSIoTMQTTClient.configureConnectDisconnectTimeout(10)  # 10 sec
myAWSIoTMQTTClient.configureMQTTOperationTimeout(5)  # 5 sec

# Connect and subscribe to AWS IoT
myAWSIoTMQTTClient.connect()

# Publish to the same topic in a loop forever
while True:
    message = {}
    message['message'] =  conn.recv(1024)
    message['device_name'] = CLIENT_ID
    message_json = json.dumps(message)
    myAWSIoTMQTTClient.publish(TOPIC, message_json, 1)
    print('Published topic %s: %s\n' % (TOPIC, message_json))
    time.sleep(10)
