# Python 3 server example
import base64
from collections import defaultdict
from enum import Enum
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import urlparse, unquote
import models
import pendulum



hostName = "localhost"
serverPort = 8666




# query_components = { "imsi" : ["Hello"] }

class State(Enum):
    idle = 1
    announced = 2
    data_sent = 3
    wait_for_announce_ack = 4

class Device:
    def __init__(self):
        self.state = State.idle

        self.status_sent = False
        self.unacked_packtes = 0
        self.server_in_sync = False
        self.last_acked_packet_id = 240

        self.last_announce = pendulum.from_timestamp(0, "UTC")




states = defaultdict(Device)

def uplink(deveui):
    msg = f"uplink for {deveui}"
    pkt = bytes()
    print(msg)
    device = states[deveui]

    now = pendulum.now("UTC")
    time_since_last_announce = (now-device.last_announce).total_seconds()
    if device.state == State.idle:
        print(f"announce for {deveui}")
        pkt = models.create_announce_packet()
        device.state = State.wait_for_announce_ack
    elif device.state == State.announced:
        if device.status_sent is False:
            print(f"status for {deveui}")
            pkt = models.create_status_packet(models.StatusPacket(sensor_time_sec=100))
            device.status_sent = True
            device.state = State.idle
        elif device.status_sent is True:
            #FIXME: Configuration: We alsways request acks
            #ack = True if device.unacked_packtes > 7 else False
            ack = True
            if device.unacked_packtes > 8:
                print(f"{deveui} for too many unacked packets")
                device.unacked_packtes = 0
                device.in_resend = True
                device.state = State.idle
                return pkt

            device.unacked_packtes += 1
            id = device.last_acked_packet_id + device.unacked_packtes
            print(f"{deveui} send data packet with ack={ack} and id = {id}")
            pkt = models.create_data_packet(ack, id)
            if ack:
                device.state = State.wait_for_announce_ack
    else:
        print("reset")
        device.state = device.state.idle

    return pkt


def downlink(deveui, base64Data, fPort):
    msg = f"{deveui} downlink"
    data = base64.b64decode(base64Data, validate=True)
    device = states[deveui]
    if device.state == State.wait_for_announce_ack:
        if fPort != 17:
            print(f"{deveui} error: waiting for announce ack but fPort is not 17")
            return
        print("{deveui} got ACK")
        device.state = State.announced
        if data == 0xFFFF:
            print(f"{deveui} server reset")
            device.unacked_packtes = 0
        else:
            server_last_full_pkt_id = int.from_bytes(data[0:1], "little", signed=False)
            acked = (server_last_full_pkt_id - device.last_acked_packet_id)%256
            print(f"{deveui} Server {server_last_full_pkt_id} client{device.last_acked_packet_id} Unacked: {device.unacked_packtes} server ACK{acked}")
            if acked >= 0:
                if device.unacked_packtes < acked:
                    print(f"{deveui} error : acked={acked} is bigger than unacked packets={device.unacked_packtes}")
                else:
                    print(f"{deveui} server acks {(acked)} packets")
                    if acked > 0 or device.server_in_sync:
                        device.server_in_sync = True
                        device.unacked_packtes = 0
                    else:
                        device.unacked_packtes -= acked
                    device.last_acked_packet_id = server_last_full_pkt_id



    return bytes(msg, "utf-8")


def handler(resource):
    query = urlparse(resource).query
    path = urlparse(resource).path
    qq = dict(qc.split("=") for qc in query.split("&"))
    deveui = qq["deveui"]
    if "uplink" in path:
        return uplink(deveui)

    if "downlink" in path:
        return downlink(deveui, unquote(qq["data"]), int(qq["fPort"]))


class MyServer(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header('Content-type', 'text/plain')
        self.end_headers()
        self.wfile.write(handler(self.path))



if __name__ == "__main__":
    webServer = HTTPServer((hostName, serverPort), MyServer)
    print("Server started http://%s:%s" % (hostName, serverPort))

    try:
        webServer.serve_forever()
    except KeyboardInterrupt:
        pass

    webServer.server_close()
    print("Server stopped.")