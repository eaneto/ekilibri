import io
from typing import Dict
from urllib.error import HTTPError
from urllib.request import Request, urlopen

import pytest
import requests

from common import EKILIBRI_URL
from ekilibri_setup import kill_process, setup_ekilibri_server


@pytest.mark.parametrize(
    "configuration",
    [
        pytest.param("least-connections", marks=pytest.mark.least_connections),
        pytest.param("round-robin", marks=pytest.mark.round_robin),
    ],
)
def test_echo_server(request, configuration):
    pid = setup_ekilibri_server(request, f"tests/ekilibri-{configuration}.toml")
    try:
        payload = "test parsing this info"

        headers = {"Content-Length": str(len(payload))}

        assert_echo_ok(headers, payload)
    finally:
        kill_process(pid)


@pytest.mark.parametrize(
    "configuration",
    [
        pytest.param("least-connections", marks=pytest.mark.least_connections),
        pytest.param("round-robin", marks=pytest.mark.round_robin),
    ],
)
def test_echo_server_with_content_type(request, configuration):
    pid = setup_ekilibri_server(request, f"tests/ekilibri-{configuration}.toml")
    try:
        payload = """{"key": "value"}"""

        headers = {
            "Content-Length": str(len(payload)),
            "Content-Type": "application/json",
        }

        assert_echo_ok(headers, payload, "application/json")
    finally:
        kill_process(pid)


@pytest.mark.parametrize(
    "configuration",
    [
        pytest.param("least-connections", marks=pytest.mark.least_connections),
        pytest.param("round-robin", marks=pytest.mark.round_robin),
    ],
)
def test_echo_server_without_content_type(request, configuration):
    pid = setup_ekilibri_server(request, f"tests/ekilibri-{configuration}.toml")
    try:
        payload = """{"key": "value"}"""

        headers = {"Content-Type": "application/json"}

        url = EKILIBRI_URL + "/echo"
        data = io.BytesIO(payload.encode("utf-8"))

        request = Request(url, data=data, headers=headers)
        with pytest.raises(HTTPError) as error:
            urlopen(request)

        assert "411" in str(error)
    finally:
        kill_process(pid)


@pytest.mark.parametrize(
    "configuration",
    [
        pytest.param("least-connections", marks=pytest.mark.least_connections),
        pytest.param("round-robin", marks=pytest.mark.round_robin),
    ],
)
def test_echo_server_with_big_body(request, configuration):
    pid = setup_ekilibri_server(request, f"tests/ekilibri-{configuration}.toml")
    try:
        payload = "test parsing this info" * 1000

        headers = {"Content-Length": str(len(payload))}

        assert_echo_ok(headers, payload)
    finally:
        kill_process(pid)


@pytest.mark.parametrize(
    "configuration",
    [
        pytest.param("least-connections", marks=pytest.mark.least_connections),
        pytest.param("round-robin", marks=pytest.mark.round_robin),
    ],
)
def test_echo_server_with_huge_body(request, configuration):
    pid = setup_ekilibri_server(request, f"tests/ekilibri-{configuration}.toml")
    try:
        payload = "test parsing this info" * 10000

        headers = {"Content-Length": str(len(payload))}

        assert_echo_ok(headers, payload)
    finally:
        kill_process(pid)


@pytest.mark.parametrize(
    "configuration",
    [
        pytest.param("least-connections", marks=pytest.mark.least_connections),
        pytest.param("round-robin", marks=pytest.mark.round_robin),
    ],
)
def test_post_not_found(request, configuration):
    pid = setup_ekilibri_server(request, f"tests/ekilibri-{configuration}.toml")
    try:
        response = requests.post(EKILIBRI_URL + "/not-found", data="data")
        assert response.status_code == 404
        assert response.text == ""
    finally:
        kill_process(pid)


def assert_echo_ok(
    headers: Dict[str, str], payload: str, content_type: str = "text/plain"
):
    response = requests.post(EKILIBRI_URL + "/echo", headers=headers, data=payload)

    assert response.status_code == 200
    assert response.headers["Content-Length"] == str(len(payload))
    assert response.headers["Content-Type"] == content_type
    assert response.text == payload
