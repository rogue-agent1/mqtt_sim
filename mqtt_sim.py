#!/usr/bin/env python3
"""MQTT protocol simulator — publish/subscribe IoT messaging.

Implements MQTT 3.1.1 broker with QoS 0/1/2, retained messages, will messages,
topic wildcards (+/#), session persistence, and packet encode/decode.

Usage: python mqtt_sim.py [--test]
"""

import sys, struct, time
from collections import defaultdict
from enum import IntEnum

class PacketType(IntEnum):
    CONNECT = 1; CONNACK = 2; PUBLISH = 3; PUBACK = 4
    PUBREC = 5; PUBREL = 6; PUBCOMP = 7; SUBSCRIBE = 8
    SUBACK = 9; UNSUBSCRIBE = 10; UNSUBACK = 11
    PINGREQ = 12; PINGRESP = 13; DISCONNECT = 14

class QoS(IntEnum):
    AT_MOST_ONCE = 0; AT_LEAST_ONCE = 1; EXACTLY_ONCE = 2

def encode_remaining_length(length):
    encoded = bytearray()
    while True:
        byte = length % 128
        length //= 128
        if length > 0:
            byte |= 0x80
        encoded.append(byte)
        if length == 0:
            break
    return bytes(encoded)

def decode_remaining_length(data, offset):
    multiplier = 1
    value = 0
    while True:
        byte = data[offset]
        offset += 1
        value += (byte & 0x7F) * multiplier
        if not (byte & 0x80):
            break
        multiplier *= 128
    return value, offset

def encode_utf8_string(s):
    encoded = s.encode('utf-8')
    return struct.pack('>H', len(encoded)) + encoded

def decode_utf8_string(data, offset):
    length = struct.unpack('>H', data[offset:offset+2])[0]
    offset += 2
    return data[offset:offset+length].decode('utf-8'), offset + length

def topic_matches(pattern, topic):
    """MQTT topic matching with + and # wildcards."""
    p_parts = pattern.split('/')
    t_parts = topic.split('/')
    pi = ti = 0
    while pi < len(p_parts):
        if p_parts[pi] == '#':
            return True
        if ti >= len(t_parts):
            return False
        if p_parts[pi] != '+' and p_parts[pi] != t_parts[ti]:
            return False
        pi += 1
        ti += 1
    return ti == len(t_parts)

class MQTTClient:
    def __init__(self, client_id, clean_session=True):
        self.client_id = client_id
        self.clean_session = clean_session
        self.subscriptions = {}  # topic_filter -> qos
        self.inbox = []
        self.connected = False
        self.will_topic = None
        self.will_message = None
        self.will_qos = QoS.AT_MOST_ONCE
        self.will_retain = False
        self.packet_id_counter = 0
        self.pending_ack = {}  # packet_id -> message (QoS 1)
        self.pending_rec = {}  # packet_id -> message (QoS 2)

    def next_packet_id(self):
        self.packet_id_counter = (self.packet_id_counter % 65535) + 1
        return self.packet_id_counter

class MQTTBroker:
    """In-memory MQTT broker."""
    def __init__(self):
        self.clients = {}  # client_id -> MQTTClient
        self.retained = {}  # topic -> (message, qos)
        self.sessions = {}  # client_id -> subscriptions (persistent)

    def connect(self, client):
        if client.clean_session and client.client_id in self.sessions:
            del self.sessions[client.client_id]
        elif not client.clean_session and client.client_id in self.sessions:
            client.subscriptions = self.sessions[client.client_id]
        self.clients[client.client_id] = client
        client.connected = True
        # Deliver retained messages for existing subscriptions
        for pattern, qos in client.subscriptions.items():
            for topic, (msg, rqos) in self.retained.items():
                if topic_matches(pattern, topic):
                    eff_qos = min(qos, rqos)
                    client.inbox.append((topic, msg, eff_qos, True))
        return True

    def disconnect(self, client_id):
        client = self.clients.get(client_id)
        if not client:
            return
        if client.will_topic and client.connected:
            pass  # will only on abnormal disconnect
        if not client.clean_session:
            self.sessions[client_id] = client.subscriptions.copy()
        client.connected = False
        del self.clients[client_id]

    def abnormal_disconnect(self, client_id):
        client = self.clients.get(client_id)
        if not client:
            return
        if client.will_topic:
            self.publish(client_id, client.will_topic, client.will_message,
                        client.will_qos, client.will_retain)
        if not client.clean_session:
            self.sessions[client_id] = client.subscriptions.copy()
        client.connected = False
        del self.clients[client_id]

    def subscribe(self, client_id, topic_filter, qos):
        client = self.clients.get(client_id)
        if not client:
            return None
        client.subscriptions[topic_filter] = qos
        # Send retained messages
        for topic, (msg, rqos) in self.retained.items():
            if topic_matches(topic_filter, topic):
                eff_qos = min(qos, rqos)
                client.inbox.append((topic, msg, eff_qos, True))
        return min(qos, 2)

    def unsubscribe(self, client_id, topic_filter):
        client = self.clients.get(client_id)
        if client and topic_filter in client.subscriptions:
            del client.subscriptions[topic_filter]

    def publish(self, publisher_id, topic, message, qos=QoS.AT_MOST_ONCE, retain=False):
        if retain:
            if message:
                self.retained[topic] = (message, qos)
            elif topic in self.retained:
                del self.retained[topic]

        delivered = 0
        for cid, client in self.clients.items():
            if not client.connected:
                continue
            for pattern, sub_qos in client.subscriptions.items():
                if topic_matches(pattern, topic):
                    eff_qos = min(qos, sub_qos)
                    client.inbox.append((topic, message, eff_qos, False))
                    delivered += 1
                    break
        return delivered

# --- Tests ---

def test_topic_matching():
    assert topic_matches("a/b/c", "a/b/c")
    assert not topic_matches("a/b/c", "a/b/d")
    assert topic_matches("a/+/c", "a/b/c")
    assert topic_matches("a/+/c", "a/x/c")
    assert not topic_matches("a/+/c", "a/b/d")
    assert topic_matches("a/#", "a/b/c")
    assert topic_matches("a/#", "a")
    assert topic_matches("#", "any/topic/here")
    assert topic_matches("+/+", "a/b")
    assert not topic_matches("+/+", "a/b/c")

def test_pubsub_basic():
    broker = MQTTBroker()
    c1 = MQTTClient("pub1")
    c2 = MQTTClient("sub1")
    broker.connect(c1)
    broker.connect(c2)
    broker.subscribe("sub1", "sensors/temp", QoS.AT_MOST_ONCE)
    n = broker.publish("pub1", "sensors/temp", "22.5C")
    assert n == 1
    assert len(c2.inbox) == 1
    assert c2.inbox[0][1] == "22.5C"

def test_wildcard_sub():
    broker = MQTTBroker()
    c = MQTTClient("sub")
    broker.connect(c)
    broker.subscribe("sub", "home/+/temperature", QoS.AT_LEAST_ONCE)
    broker.publish("x", "home/living/temperature", "21C")
    broker.publish("x", "home/bedroom/temperature", "19C")
    broker.publish("x", "home/living/humidity", "45%")
    assert len(c.inbox) == 2

def test_retained():
    broker = MQTTBroker()
    p = MQTTClient("pub")
    broker.connect(p)
    broker.publish("pub", "status/online", "true", retain=True)
    s = MQTTClient("sub")
    broker.connect(s)
    broker.subscribe("sub", "status/online", QoS.AT_MOST_ONCE)
    assert len(s.inbox) == 1
    assert s.inbox[0][3] == True  # retained flag

def test_will_message():
    broker = MQTTBroker()
    c1 = MQTTClient("device1")
    c1.will_topic = "devices/device1/status"
    c1.will_message = "offline"
    c2 = MQTTClient("monitor")
    broker.connect(c1)
    broker.connect(c2)
    broker.subscribe("monitor", "devices/#", QoS.AT_MOST_ONCE)
    broker.abnormal_disconnect("device1")
    assert any(m[1] == "offline" for m in c2.inbox)

def test_session_persistence():
    broker = MQTTBroker()
    c = MQTTClient("persistent", clean_session=False)
    broker.connect(c)
    broker.subscribe("persistent", "data/#", QoS.AT_LEAST_ONCE)
    broker.disconnect("persistent")
    c2 = MQTTClient("persistent", clean_session=False)
    broker.connect(c2)
    assert "data/#" in c2.subscriptions

def test_qos_downgrade():
    broker = MQTTBroker()
    c = MQTTClient("sub")
    broker.connect(c)
    broker.subscribe("sub", "topic", QoS.AT_MOST_ONCE)
    broker.publish("x", "topic", "msg", QoS.EXACTLY_ONCE)
    assert c.inbox[0][2] == QoS.AT_MOST_ONCE  # downgraded

def test_remaining_length():
    for v in [0, 1, 127, 128, 16383, 16384, 2097151, 268435455]:
        encoded = encode_remaining_length(v)
        decoded, _ = decode_remaining_length(encoded, 0)
        assert decoded == v, f"{v} -> {encoded} -> {decoded}"

if __name__ == "__main__":
    if "--test" in sys.argv or len(sys.argv) == 1:
        test_topic_matching()
        test_pubsub_basic()
        test_wildcard_sub()
        test_retained()
        test_will_message()
        test_session_persistence()
        test_qos_downgrade()
        test_remaining_length()
        print("All tests passed!")
