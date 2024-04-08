import struct
import typing
from dataclasses import dataclass



def decode_status_packet(data: bytes):
    # midi bolus status packet
    # see struct in midibolus firmware:
    # https://bitbucket.org/smaxtec/midibolus/src/develop/firmware/SharedCommunication/comm_build_status_packet.h
    min_data_len = 43
    if (len(data) < min_data_len):
        raise ValueError(
            f'Status packet data must contain at least {min_data_len} bytes')

    status = StatusPacket()

    (
        status.sensor_time_sec,
        firmware_version_and_position,
        status.checksum_lower_image,
        status.checksum_upper_image,
        status.packet_creation_time_sec
    ) = struct.unpack('<IIIII', data[0:20])

    position_mask = 1 << 31
    status.firmware_version = firmware_version_and_position & ~position_mask
    is_upper = (firmware_version_and_position & position_mask) != 0
    if is_upper:
        status.firmware_position = 0
    else:
        status.firmware_position = 1
    (
        status.reboot_count,
        status.reboot_source
    ) = struct.unpack('<HB', data[20:23])
    (
        transmit_time_hundredth_sec,
        receive_time_hundredth_sec
    ) = struct.unpack('<HH', data[23:27])
    status.transmit_time_milliseconds = transmit_time_hundredth_sec * 10
    status.receive_time_milliseconds = receive_time_hundredth_sec * 10

    (
        status.rx_errors,
        status.downlink_received_signal_strength_decaying_average,
        status.downlink_signal_noise_ratio_decaying_average,
        status.number_of_announcements_sent,
        status.number_of_data_status_resends,
        status.number_of_generic_downlink_packets_received
    ) = struct.unpack('<BhbBBB', data[27:34])

    (
        status.measurement_scheduling_period,
        status.number_of_accelerometer_overflows,
        status.number_of_i2c_errors,
        status.number_of_magnet_activations,
        status.number_of_magnet_reactivations_of_active_sensor
    ) = struct.unpack('<HBBBB', data[34:40])

    (
        status.gravity_estimate_x,
        status.gravity_estimate_y,
        status.gravity_estimate_z
    ) = struct.unpack('<bbb', data[40:43])

    if len(data) >= 44:
        status.clock_errors = data[43]
    return status


@dataclass
class StatusPacket:
    sensor_time_sec: int = 0
    firmware_version: int = 0
    firmware_position: int = 0
    checksum_lower_image: int = 0
    checksum_upper_image: int = 0
    packet_creation_time_sec: int = 0
    reboot_count: int = 0
    reboot_source: int = 0
    transmit_time_milliseconds: int = 0
    receive_time_milliseconds: int = 0
    rx_errors: int = 0
    downlink_received_signal_strength_decaying_average: int = 0
    downlink_signal_noise_ratio_decaying_average: int = 0
    number_of_announcements_sent: int = 0
    number_of_data_status_resends: int = 0
    number_of_generic_downlink_packets_received: int = 0
    measurement_scheduling_period: int = 0
    number_of_accelerometer_overflows: int = 0
    number_of_i2c_errors: int = 0
    number_of_magnet_activations: int = 0
    number_of_magnet_reactivations_of_active_sensor: int = 0
    gravity_estimate_x: int = 0
    gravity_estimate_y: int = 0
    gravity_estimate_z: int = 0
    clock_errors: typing.Optional[int] = 0

def create_announce_packet():
    return bytes([0])


def create_status_packet(status):
    coded = struct.pack('<IIIIIHBHHBhbBBBHBBBBbbbb',
        status.sensor_time_sec,
        status.firmware_version,
        status.checksum_lower_image,
        status.checksum_upper_image,
        status.packet_creation_time_sec,
        status.reboot_count,
        status.reboot_source,
        status.transmit_time_milliseconds, # /10
        status.receive_time_milliseconds,# /10
        status.rx_errors,
        status.downlink_received_signal_strength_decaying_average,
        status.downlink_signal_noise_ratio_decaying_average,
        status.number_of_announcements_sent,
        status.number_of_data_status_resends,
        status.number_of_generic_downlink_packets_received,
        status.measurement_scheduling_period,
        status.number_of_accelerometer_overflows,
        status.number_of_i2c_errors,
        status.number_of_magnet_activations,
        status.number_of_magnet_reactivations_of_active_sensor,
        status.gravity_estimate_x,
        status.gravity_estimate_y,
        status.gravity_estimate_z,
        status.clock_errors
    )

    packet = bytearray(1)
    packet[0]=12
    packet[1:1]= coded
    return packet




def create_data_packet(ack, packet_id):
    return bytearray([2 if ack else 1, packet_id % 256, 0, 1, 2, 3, 4, 5, 6, 7])

#
#
# if __name__ == "__main__":
#     sp = StatusPacket()
#     sp.sensor_time_sec=666
#     sp.number_of_i2c_errors=66
#     print(create_status_packet(sp))
#     print(decode_status_packet(create_status_packet(sp)[1:]))
#
#     print(create_announce_packet())
#
#     lst = [simstats.Device(1), simstats.Device(2)]
#
#     print(sorted(lst, key=lambda x : x.last_seen))
#     exit(0)