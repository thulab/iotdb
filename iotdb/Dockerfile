FROM java:8

RUN mkdir /iotdb
COPY . /iotdb
RUN mkdir /iotdb/data

EXPOSE 6667

CMD ["sh", "-c", "/iotdb/bin/start-server.sh"]