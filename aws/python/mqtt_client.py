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
        self._client_id = None
        self._aws_host = None
        self._aws_port = None
        self._root_CA_path = None
        self._private_key_path = None
        self._cert_path = None
        self._socket_ip = None
        self._socket_port = None
        self._myAWSIoTMQTTClient = None
        self._socket_connection = None

    @classmethod
    def start(cls, info_file):
        """Start the socket server and connect the MQTT client.
        Arguments:
        info_file (str): Fully qualified name of file containing all
                        the information required to start the services.
        """
        mqtt_client = cls()
        connection_parameters = mqtt_client._get_mqtt_parameters_from_file(info_file)
        mqtt_client._connect_MQTT(**connection_parameters)
        set_trace()

    def _get_mqtt_parameters_from_file(self, filename):
        """Get the AWS MQTT connection parameters from a file.

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

    def _connect_socket(self, ip, port):
        """Create a local socket server.

        Arguments:
        port (int): Port number for socket server to listen.

        Returns a socket connection.
        """
        host_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        host_socket.bind((ip, port))
        host_socket.listen(1)
        conn, addr = host_socket.accept()
        return conn

if __name__=="__main__":
    MQTTClient.start("connection_info.json")
