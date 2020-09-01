from AWSIoTPythonSDK.MQTTLib import AWSIoTMQTTClient
import time
import json
import socket
from pdb import set_trace
"""
Brent Maranzano
"""

class MQTTClient(object):
    """Object that listens on a local socket and relays traffic
    to an MQTT connection to AWS IoT services.
    """

    def __init__(self):
        self._myAWSIoTMQTTClient = None
        self._socket_connection = None

    @classmethod
    def start(cls, mqtt_param_file=None, socket_param_file=None):
        """Start the socket server and connect the MQTT client.

        Arguments:
        mqtt_param_file (str): Fully qualified name of file containing MQTT params
        socket_param_file (str): Fully qualified name of file containing socket params
        """
        mqtt_client = cls()
        socket_params = mqtt_client._get_parameters_from_file(socket_param_file)
        mqtt_client._socket_connection = mqtt_client._connect_socket(**socket_params)
        mqtt_params = mqtt_client._get_parameters_from_file(mqtt_param_file)
        mqtt_client._myAWSIoTMQTTClient = mqtt_client._connect_MQTT(**mqtt_params)

        #mqtt_client._myAWSIoTMQTTClient.publish("test", json.dumps({"message": "hello world"}), 1)

        while True:
            message =  mqtt_client._socket_connection.recv(1024)
            message = message.decode("utf-8")
            mqtt_client._myAWSIoTMQTTClient.publish("test", message, 1)
            time.sleep(2)

    def _get_parameters_from_file(self, filename):
        """Get the parameters from a JSON file

        Arguments:
        filename (str): Fully qualified name of the parameter file.

        Returns (dict): Dictionary of parameters
        """
        with open(filename) as fObj:
            return json.loads(fObj.read())

    def _connect_MQTT(self, client_id="", host="", port="",
                      root_CA_path="", private_key_path="", cert_path=""):
        """Create a connection to the AWS IoT MQTT broker.
        Returns a connected MQTT client.
        """
        client = AWSIoTMQTTClient(client_id)
        client.configureEndpoint(host, port)
        client.configureCredentials(root_CA_path, private_key_path, cert_path)

        # AWSIoTMQTTClient connection configuration
        client.configureAutoReconnectBackoffTime(1, 32, 20)

        # client.configureOfflinePublishQueueing(-1)  # Infinite offline Publish queueing
        client.configureDrainingFrequency(2)  # Draining: 2 Hz
        client.configureConnectDisconnectTimeout(10)  # 10 sec
        client.configureMQTTOperationTimeout(5)  # 5 sec

        # Connect and subscribe to AWS IoT
        client.connect()
        return client

    def _connect_socket(self, ip="", port=50007):
        """Create a local socket server.

        Arguments:
        ip (string): IP address of host.
        port (int): Port number for socket server to listen.

        Returns a socket connection.
        """
        print("socket")
        host_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        host_socket.bind((ip, port))
        host_socket.listen(1)
        print("listen on {}:{}".format(ip, port))
        conn, addr = host_socket.accept()
        print("connected {}".format(addr))
        return conn

if __name__=="__main__":
    print("test")
    MQTTClient.start(mqtt_param_file="mqtt_connection_info.json",
                     socket_param_file="socket_connection_info.json")
