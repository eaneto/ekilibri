import io
from time import sleep
from urllib.error import HTTPError
from urllib.request import Request, urlopen

import pytest
import requests

from ekilibri_setup import (
    find_and_kill_command_server,
    kill_process,
    setup_ekilibri_server,
)

URL = "http://localhost:8080"


def test_multiple_get_request_to_three_servers(request):
    pid = setup_ekilibri_server(request, "tests/ekilibri-least-connections.toml")
    try:
        for _ in range(100):
            response = requests.get(URL + "/health")
            assert response.status_code == 200
    finally:
        kill_process(pid)


def test_multiple_get_request_to_three_servers_with_two_failed(request):
    pid = setup_ekilibri_server(request, "tests/ekilibri-least-connections.toml")
    try:
        response = requests.get(URL + "/health")
        assert response.status_code == 200

        find_and_kill_command_server()
        # Wait the fail window for ekilibri to remove the server
        sleep(10)
        for _ in range(100):
            response = requests.get(URL + "/health")
            assert response.status_code == 200

        find_and_kill_command_server()
        # Wait the fail window for ekilibri to remove the server
        sleep(10)
        for _ in range(100):
            response = requests.get(URL + "/health")
            assert response.status_code == 200
    finally:
        kill_process(pid)


def test_echo_server(request):
    pid = setup_ekilibri_server(request, "tests/ekilibri-least-connections.toml")
    try:
        payload = "test parsing this info"

        headers = {"Content-Length": str(len(payload))}

        response = requests.post(URL + "/echo", headers=headers, data=payload)

        assert response.status_code == 200
        assert response.headers["Content-Length"] == str(len(payload))
        assert response.headers["Content-Type"] == "text/plain"
        assert response.text == payload
    finally:
        kill_process(pid)


def test_echo_server_with_content_type(request):
    pid = setup_ekilibri_server(request, "tests/ekilibri-least-connections.toml")
    try:
        payload = """{"key": "value"}"""

        headers = {
            "Content-Length": str(len(payload)),
            "Content-Type": "application/json",
        }

        response = requests.post(URL + "/echo", headers=headers, data=payload)

        assert response.status_code == 200
        assert response.headers["Content-Length"] == str(len(payload))
        assert response.headers["Content-Type"] == "application/json"
        assert response.text == payload
    finally:
        kill_process(pid)


def test_echo_server_without_content_type(request):
    pid = setup_ekilibri_server(request, "tests/ekilibri-least-connections.toml")
    try:
        payload = """{"key": "value"}"""

        headers = {"Content-Type": "application/json"}

        url = URL + "/echo"
        data = io.BytesIO(payload.encode("utf-8"))

        request = Request(url, data=data, headers=headers)
        with pytest.raises(HTTPError) as error:
            urlopen(request)

        assert "411" in str(error)
    finally:
        kill_process(pid)


@pytest.mark.skip(
    reason="TODO: Still need to fix parsing payloads bigger than 1kb and responses over 1kb"
)
def test_echo_server_with_big_body(request):
    pid = setup_ekilibri_server(request, "tests/ekilibri-least-connections.toml")
    try:
        payload = "test parsing this info" * 1000

        headers = {"Content-Length": str(len(payload))}

        response = requests.post(URL + "/echo", headers=headers, data=payload)

        assert response.status_code == 200
        assert response.headers["Content-Length"] == str(len(payload))
        assert response.headers["Content-Type"] == "text/plain"
        assert response.text == payload
    finally:
        kill_process(pid)


@pytest.mark.skip(
    reason="TODO: Still need to fix parsing payloads bigger than 1kb and responses over 1kb"
)
def test_echo_server_with_huge_body(request):
    pid = setup_ekilibri_server(request, "tests/ekilibri-least-connections.toml")
    try:
        payload = "test parsing this info" * 10000

        headers = {"Content-Length": str(len(payload))}

        response = requests.post(URL + "/echo", headers=headers, data=payload)

        assert response.status_code == 200
        assert response.headers["Content-Length"] == str(len(payload))
        assert response.headers["Content-Type"] == "text/plain"
        assert response.text == payload
    finally:
        kill_process(pid)


def test_get_not_found(request):
    pid = setup_ekilibri_server(request, "tests/ekilibri-least-connections.toml")
    try:
        response = requests.get(URL + "/not-found")
        assert response.status_code == 404
        # TODO: Fix payload sent by the server
        # assert response.text == ""
    finally:
        kill_process(pid)


def test_post_not_found(request):
    pid = setup_ekilibri_server(request, "tests/ekilibri-least-connections.toml")
    try:
        response = requests.post(URL + "/not-found", data="data")
        assert response.status_code == 404
        # TODO: Fix payload sent by the server
        # assert response.text == ""
    finally:
        kill_process(pid)
