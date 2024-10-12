import requests


def test_echo_server():
    payload = "test parsing this info"

    headers = {"Content-Length": str(len(payload))}

    response = requests.post(
        "http://localhost:8083/echo", headers=headers, data=payload
    )

    assert response.status_code == 200
    assert response.headers["Content-Length"] == str(len(payload))
    assert response.headers["Content-Type"] == "text/plain"
    assert response.text == payload


def test_echo_server_with_big_body():
    payload = "test parsing this info" * 1000

    headers = {"Content-Length": str(len(payload))}

    response = requests.post(
        "http://localhost:8083/echo", headers=headers, data=payload
    )

    assert response.status_code == 200
    assert response.headers["Content-Length"] == str(len(payload))
    assert response.headers["Content-Type"] == "text/plain"
    assert response.text == payload


def test_echo_server_with_huge_body():
    payload = "test parsing this info" * 10000

    headers = {"Content-Length": str(len(payload))}

    response = requests.post(
        "http://localhost:8083/echo", headers=headers, data=payload
    )

    assert response.status_code == 200
    assert response.headers["Content-Length"] == str(len(payload))
    assert response.headers["Content-Type"] == "text/plain"
    assert response.text == payload


def test_get_not_found():
    response = requests.get("http://localhost:8083/not-found")
    assert response.status_code == 404
    assert response.text == ""


def test_post_not_found():
    response = requests.post("http://localhost:8083/not-found", data="data")
    assert response.status_code == 404
    assert response.text == ""
