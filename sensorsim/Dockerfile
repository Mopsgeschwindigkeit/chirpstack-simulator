FROM ubuntu


RUN apt-get update
RUN apt-get install python3 -y
RUN apt-get install -y python3-pip
COPY main.py /
COPY models.py /
COPY simstats.py /

RUN python3 -m pip install urllib3 pendulum
RUN python3 -m pip install click

ENTRYPOINT python3 main.py