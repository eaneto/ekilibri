import pytest

from common import assert_health, assert_health_multiple_status, wait_fail_window
from ekilibri_setup import (
    find_and_kill_all_command_servers,
    kill_process,
    setup_command_server,
    setup_ekilibri_server,
)


@pytest.mark.parametrize(
    ("configuration", "ports"),
    [
        pytest.param("least-connections", (8081,), marks=pytest.mark.least_connections),
        pytest.param(
            "least-connections",
            (
                8081,
                8082,
            ),
            marks=pytest.mark.least_connections,
        ),
        pytest.param(
            "least-connections",
            (
                8081,
                8082,
                8083,
            ),
            marks=pytest.mark.least_connections,
        ),
        pytest.param("round-robin", (8081,), marks=pytest.mark.round_robin),
        pytest.param(
            "round-robin",
            (
                8081,
                8082,
            ),
            marks=pytest.mark.round_robin,
        ),
        pytest.param(
            "round-robin",
            (
                8081,
                8082,
                8083,
            ),
            marks=pytest.mark.round_robin,
        ),
    ],
)
def test_startup_with_failed_servers(request, configuration, ports):
    find_and_kill_all_command_servers()

    pid = setup_ekilibri_server(request, f"tests/ekilibri-{configuration}.toml")
    pids = []
    try:
        assert_health_multiple_status(100, statuses={502, 504})

        for port in ports:
            pids.append(setup_command_server(request, port))

        wait_fail_window()

        assert_health(10, status=200)
    finally:
        kill_process(pid)
        for pid in pids:
            kill_process(pid)
