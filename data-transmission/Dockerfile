FROM ubuntu:latest

MAINTAINER YUFEI ZHANG 'zhangyufei0202@gmail.com'

RUN apt-get update
RUN apt-get install -y python3.6 python3-pip
COPY ./data-producer.py /
COPY ./data-consumer.py /
COPY ./requirements.txt /
RUN pip3 install -r requirements.txt

CMD python3 data-producer.py BTC-USD test kafka:9092
