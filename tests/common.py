from time import sleep
from typing import Set

import requests

EKILIBRI_URL = "http://localhost:8080"


def assert_health(n: int, status: int):
    for _ in range(n):
        response = requests.get(EKILIBRI_URL + "/health")
        assert response.status_code == status


def assert_health_multiple_status(n: int, statuses: Set[int]):
    """Checks the response for the health endpoint validating the status code
    could be of different values."""
    for _ in range(n):
        response = requests.get(EKILIBRI_URL + "/health")
        assert response.status_code in statuses


def wait_fail_window():
    """Waits the fail window for ekilibri to remove the server from the healthy servers list."""
    sleep(5)
