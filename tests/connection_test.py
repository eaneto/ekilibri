from client import EkilibriClient


def test_connect():
    client = EkilibriClient()
    client.connect(8083)
    payload = "test parsing this info" * 1000
    received = client.send_and_receive(payload)[: len(payload)]
    assert received == payload.encode("utf-8")
