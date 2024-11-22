import pytest
import requests

URL = "http://localhost:8081"


@pytest.mark.command
def test_echo_server():
    payload = "test parsing this info"

    headers = {"Content-Length": str(len(payload))}

    response = requests.post(URL + "/echo", headers=headers, data=payload)

    assert response.status_code == 200
    assert response.headers["Content-Length"] == str(len(payload))
    assert response.headers["Content-Type"] == "text/plain"
    assert response.text == payload


@pytest.mark.command
def test_echo_server_with_content_type():
    payload = """{"key": "value"}"""

    headers = {"Content-Length": str(len(payload)), "Content-Type": "application/json"}

    response = requests.post(URL + "/echo", headers=headers, data=payload)

    assert response.status_code == 200
    assert response.headers["Content-Length"] == str(len(payload))
    assert response.headers["Content-Type"] == "application/json"
    assert response.text == payload


@pytest.mark.command
def test_echo_server_with_big_body():
    payload = "test parsing this info" * 1000

    headers = {"Content-Length": str(len(payload))}

    response = requests.post(URL + "/echo", headers=headers, data=payload)

    assert response.status_code == 200
    assert response.headers["Content-Length"] == str(len(payload))
    assert response.headers["Content-Type"] == "text/plain"
    assert response.text == payload


@pytest.mark.command
def test_echo_server_with_huge_body():
    payload = "test parsing this info" * 10000

    headers = {"Content-Length": str(len(payload))}

    response = requests.post(URL + "/echo", headers=headers, data=payload)

    assert response.status_code == 200
    assert response.headers["Content-Length"] == str(len(payload))
    assert response.headers["Content-Type"] == "text/plain"
    assert response.text == payload


@pytest.mark.command
def test_get_not_found():
    response = requests.get(URL + "/not-found")
    assert response.status_code == 404
    assert response.text == ""


@pytest.mark.command
def test_post_not_found():
    response = requests.post(URL + "/not-found", data="data")
    assert response.status_code == 404
    assert response.text == ""
