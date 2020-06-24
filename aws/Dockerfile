from python:3.6

RUN useradd -rm -d /home/aws -s /bin/bash -U -u 1000 aws
USER aws

RUN mkdir /home/aws/SDK && mkdir /home/aws/python
WORKDIR /home/aws/SDK
RUN curl https://www.amazontrust.com/repository/AmazonRootCA1.pem -o /home/aws/SDK/root-CA.crt\
    && git clone https://github.com/aws/aws-iot-device-sdk-python-v2.git

USER root
RUN cd /home/aws/SDK/aws-iot-device-sdk-python-v2\
    && python setup.py install

USER aws
COPY mqtt_client.py /home/aws/python/

WORKDIR /home/aws/python
CMD ["/bin/bash", "-c", "sleep 10000"]
#CMD ["python", "mqtt_client.py"] 
