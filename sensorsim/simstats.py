import json
import os
import random
import struct
from datetime import datetime
from io import BytesIO
import base64
import click
from confluent_kafka import (
    Consumer,
    KafkaError,
    TopicPartition,
)
from confluent_kafka.avro import AvroConsumer
from confluent_kafka.avro.serializer import SerializerError
from fastavro import (
    parse_schema,
    schemaless_reader,
    schemaless_writer,
)

MAGIC_BYTE = 0


class Device:
    def __init__(self, did):
        self.did = did
        self._fid = -1
        self.overflows = 0
        self.resends = 0
        self.last_seen = datetime.now()
        self.packets = 0
        self.fid_errors = 0

    def set_fid(self, fid):
        if self._fid > 250 and fid < self._fid:
            self.overflows += 1
        elif self._fid >= fid:
            self.resends += 1
        old_fid = self._fid
        self._fid = fid
        if old_fid == fid:
            return False
        if old_fid != -1 and fid != (old_fid + 1)%256:
            self.fid_errors += 1

    def fid(self):
        return 255 * self.overflows + self._fid

@click.command()
@click.option('--reset', default="latest", help='offset-reset')
@click.option('--from_beginning', is_flag=True, default=False, help="start from beginning, alias for --reset earliest --consumer-group [random]")
@click.option('--consumer-group', default=None)
@click.option('--bootstrap-server', default="bootstrap:9092")
@click.option('--schema-registry', default='http://avro-schemas:32406')
@click.argument('topics', default=["dev_object_beehive_midi_upload"])
def consumer(reset, consumer_group,  bootstrap_server,
             schema_registry, from_beginning, topics):
    last_run = None
    devices = dict()
    print(topics)
    c = create_consumer(from_beginning, consumer_group, False,
                        bootstrap_server, reset, schema_registry)

    subscribe_to_topics(c, topics)
    print("subscribe")
    while True:
        msg = None
        try:
            #print("poll")
            msg = c.poll(2)

        except SerializerError as e:
            print("Message deserialization failed for {}: {}".format(msg, e))
            raise

        if msg is None:
            print("none")
            continue

        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                print("eof")
                continue
            else:
                print(msg.error())
                break

        key = msg.key()
        value = msg.value()

        device_id = key["id"]
        if value["fPort"] in (1,2):

            fid = int(base64.b64decode(value["data"])[0:1].hex(), 16)


            if device_id not in devices:
                devices[device_id] = Device(device_id)

            dev = devices[device_id]

            dev.set_fid(fid)
            dev.packets += 1
            dev.last_seen = datetime.now()


        if last_run is None or (datetime.now() - last_run).total_seconds() > 2:
            os.system('cls' if os.name == 'nt' else 'clear')
            print("run")
            last_run = datetime.now()
            print("device\tlast fram<e id\tPackets\tresends\tlast-seen")
            #max_count =
            for d in sorted(devices.values(), key=lambda x : x.last_seen):
                print(print_device_line(d))
            print("")

            packet_number_median = median([x.fid() for x in devices.values()])
            packet_count_median = median([x.packets for x in devices.values()])



            #print("Errors ins Frame Number: ")
            # for d in filter(lambda x: abs(packet_number_median-x.fid()) > 20, devices.values()):
            #     print(print_device_line(d))



            print("Offline Devices:")
            offline = 0
            for d in filter(lambda x: (datetime.now() - x.last_seen).total_seconds() > 31, devices.values()):
                print(print_device_line(d))
                offline += 1

            print("Errors in Frame Count:")
            for d in filter(lambda x: x.fid_errors > 0, devices.values()):
                print(print_device_line(d))

            print("Stats:")
            print("count: " + str(len(devices)))
            print("Frame number median:" + str(packet_number_median))
            print("Frame Count median: " + str(packet_count_median))
            print("offline devices: *** " + str(offline) + " ***")
            if len(devices) > 0:
                print("Frame count max: " + str(max([x.packets for x in devices.values()])))
                print("Frame count min: " + str(min([x.packets for x in devices.values()])))
    c.close()

def median(lst):
    n = len(lst)
    s = sorted(lst)
    return (s[n//2-1]/2.0+s[n//2]/2.0, s[n//2])[n % 2] if n else None

def print_device_line(d):
    print(d.did + "\t" + str(d.fid()) + "\t" + str(d.packets) + "\t" + str(d.resends) + "\t" + str(
        (datetime.now() - d.last_seen).total_seconds()))

def create_consumer(from_beginning, consumer_group, no_avro, bootstrap_server,
                    reset, schema_registry):

    if from_beginning:
        reset = "earliest"

    if consumer_group is None:
        consumer_group = "simulator_stats_" + str(random.randint(0, 10000))

    print("using consumer group: " + consumer_group)

    if no_avro is True:
        consumer = Consumer({
            'group.id': consumer_group,
            'bootstrap.servers': bootstrap_server,
            'auto.offset.reset': reset,
        })
    else:
        consumer = AvroConsumer({
            'group.id': consumer_group,
            'bootstrap.servers': bootstrap_server,
            'schema.registry.url': schema_registry,
            'auto.offset.reset': reset,
        })

    print(schema_registry)
    return consumer


def subscribe_to_topics(c, topics, tp=0):

    topics = [topics]
    print(topics)
    if tp == 0:
        print("subscribe to " + str(topics))
        c.subscribe(topics)
    else:
        tps = []
        for i in range(0, int(len(topics)/2)):
            topic = topics[i]
            part = int(topics[i+1])
            tps.append(TopicPartition(topic, part))
        print("assing " + str(tps))
        c.assign(tps)


def decode_message(key, value, decode):

    if decode == "cachetable":
        key = _parse_avro(key, schema=schema_key)
        if value:
            value = _parse_avro(value, schema=schema_cachetable)
    elif decode == "upload":
        ts_f = ["server_time", "basestation_time", "parse_time", "data_min_ts",
                "readout_time", "data_max_ts"]
        for f in ts_f:
            if f in value:
                value[f] = str(datetime.utcfromtimestamp(int(value[f])))
    elif decode == "metriclist":
        for d in value["data"]:
            d["ts"] = str(datetime.utcfromtimestamp(int(d["ts"])))

    return key, value


def write_to_file(to_file, bytesarray, value):

    if bytesarray:
        file_object = open(to_file, 'wb')
        file_object.write(value)
    else:
        file_object = open(to_file, 'w')
        file_object.write(json.dumps(value))

    file_object.close()


def format_output(topics, msg, key, value, to_json):

    ts = msg.timestamp()[1] / 1000
    dt = datetime.utcfromtimestamp(ts)

    if len(topics) > 1 or topics[0][0] == "^":
        topic = msg.topic() + ":"
    else:
        topic =""

    if to_json:
        return json.dumps({
            "topic": topic,
            "message_datetime": str(dt),
            "message_timestamp": ts,
            "partition": msg.partition(),
            "key": key,
            "value": value,
        })
    else:
        return (topic + str(dt) + ":" + str(msg.timestamp()) + ":" +
                str(msg.partition()) + ":" + str(key) + "=" + str(value))


# hacked avro parser from anthillprocessing-stream
def _parse_avro(b, schema):
    payload = BytesIO(b)
    magic, schema_id = struct.unpack('>bI', payload.read(5))
    if magic != MAGIC_BYTE:
        raise ValueError("message does not start with magic byte")
    return schemaless_reader(payload, schema)


schema_key = json.loads("""
{
    "namespace": "com.smaxtec.beehive.models.avro",
    "type": "record",
    "name": "Id",
    "fields": [
        {
            "name": "id",
            "type": "string"
        },
        {
            "name": "organisation_id",
            "type": [
                "string",
                "null"
            ],
            "default": "null"
        }
    ]
}
""")

schema_cachetable = json.loads("""
{
    "name": "DataStore",
    "type": "record",
    "namespace": "com.smaxtec.my.avro.model",
    "fields": [
        {
        "name": "_timestamps",
        "type": {
            "type": "array",
            "items": "int"
        }
        },
        {
        "name": "_timestamp_offsets",
        "type": {
            "type": "array",
            "items": "int"
        }
        },

        {
        "name": "_float_value_map",
        "type": {
            "type": "map",
            "values":
                {
                "items": [
                    "float"
                ],
                "type": "array"
                }
        }
        },

        {
        "name": "_int_value_map",
        "type": {
            "type": "map",
            "values":
                {
                "items": [
                    "int"
                ],
                "type": "array"
                }
        }
        },

        {
        "name": "create_ts",
        "type": "int"
        }
    ]
}
""")

if __name__ == '__main__':
    consumer()
