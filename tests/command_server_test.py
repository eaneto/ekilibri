import io
from urllib.error import HTTPError
from urllib.request import Request, urlopen

import pytest
import requests


def test_echo_server():
    payload = "test parsing this info"

    headers = {"Content-Length": str(len(payload))}

    response = requests.post(
        "http://localhost:8081/echo", headers=headers, data=payload
    )

    assert response.status_code == 200
    assert response.headers["Content-Length"] == str(len(payload))
    assert response.headers["Content-Type"] == "text/plain"
    assert response.text == payload


def test_echo_server_with_content_type():
    payload = """{"key": "value"}"""

    headers = {"Content-Length": str(len(payload)), "Content-Type": "application/json"}

    response = requests.post(
        "http://localhost:8081/echo", headers=headers, data=payload
    )

    assert response.status_code == 200
    assert response.headers["Content-Length"] == str(len(payload))
    assert response.headers["Content-Type"] == "application/json"
    assert response.text == payload


def test_echo_server_without_content_type():
    payload = """{"key": "value"}"""

    headers = {"Content-Type": "application/json"}

    url = "http://localhost:8081/echo"
    data = io.BytesIO(payload.encode("utf-8"))

    request = Request(url, data=data, headers=headers)
    with pytest.raises(HTTPError) as error:
        urlopen(request)

    assert "411" in str(error)


def test_echo_server_with_big_body():
    payload = "test parsing this info" * 1000

    headers = {"Content-Length": str(len(payload))}

    response = requests.post(
        "http://localhost:8081/echo", headers=headers, data=payload
    )

    assert response.status_code == 200
    assert response.headers["Content-Length"] == str(len(payload))
    assert response.headers["Content-Type"] == "text/plain"
    assert response.text == payload


def test_echo_server_with_huge_body():
    payload = "test parsing this info" * 10000

    headers = {"Content-Length": str(len(payload))}

    response = requests.post(
        "http://localhost:8081/echo", headers=headers, data=payload
    )

    assert response.status_code == 200
    assert response.headers["Content-Length"] == str(len(payload))
    assert response.headers["Content-Type"] == "text/plain"
    assert response.text == payload


def test_get_not_found():
    response = requests.get("http://localhost:8081/not-found")
    assert response.status_code == 404
    assert response.text == ""


def test_post_not_found():
    response = requests.post("http://localhost:8081/not-found", data="data")
    assert response.status_code == 404
    assert response.text == ""
