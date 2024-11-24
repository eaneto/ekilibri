import pytest
import requests

from common import EKILIBRI_URL, assert_health
from ekilibri_setup import kill_process, setup_ekilibri_server


@pytest.mark.parametrize(
    "configuration",
    [
        pytest.param("least-connections", marks=pytest.mark.least_connections),
        pytest.param("round-robin", marks=pytest.mark.round_robin),
    ],
)
def test_multiple_get_request_to_three_servers(request, configuration):
    pid = setup_ekilibri_server(request, f"tests/ekilibri-{configuration}.toml")
    try:
        assert_health(100, status=200)
    finally:
        kill_process(pid)


@pytest.mark.parametrize(
    "configuration",
    [
        pytest.param("least-connections", marks=pytest.mark.least_connections),
        pytest.param("round-robin", marks=pytest.mark.round_robin),
    ],
)
def test_multiple_get_request_with_timeout(request, configuration):
    pid = setup_ekilibri_server(request, f"tests/ekilibri-{configuration}.toml")
    try:
        for _ in range(10):
            response = requests.get(EKILIBRI_URL + "/sleep")
            assert response.status_code == 504
    finally:
        kill_process(pid)


@pytest.mark.parametrize(
    "configuration",
    [
        pytest.param("least-connections", marks=pytest.mark.least_connections),
        pytest.param("round-robin", marks=pytest.mark.round_robin),
    ],
)
def test_get_not_found(request, configuration):
    pid = setup_ekilibri_server(request, f"tests/ekilibri-{configuration}.toml")
    try:
        response = requests.get(EKILIBRI_URL + "/not-found")
        assert response.status_code == 404
        assert response.text == ""
    finally:
        kill_process(pid)
