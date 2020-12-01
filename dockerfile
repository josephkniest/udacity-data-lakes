# Image

FROM ubuntu

# CPython and udacity dependencies installation

RUN apt-get update

RUN apt-get install -y sudo

RUN sudo apt-get install -y python3.7

RUN sudo apt-get install -y python3-pip

RUN sudo apt-get install -y libpq-dev

RUN pip3 install psycopg2

RUN pip3 install sql

RUN pip3 install boto3

RUN pip3 install ipython-sql

# Drop udacity project files into image

RUN mkdir /root/udacity

COPY udacity/ /root/udacity

# Install some GNU essentials into image

RUN sudo apt-get install -y nano

