import pytest

from common import assert_health, assert_health_multiple_status, wait_fail_window
from ekilibri_setup import (
    find_and_kill_all_command_servers,
    find_and_kill_command_server,
    kill_process,
    setup_command_server,
    setup_ekilibri_server,
)


@pytest.mark.parametrize(
    "configuration",
    [
        pytest.param("least-connections", marks=pytest.mark.least_connections),
        pytest.param("round-robin", marks=pytest.mark.round_robin),
    ],
)
def test_multiple_get_request_to_three_servers_with_one_failed(request, configuration):
    pid = setup_ekilibri_server(request, f"tests/ekilibri-{configuration}.toml")
    try:
        assert_health(10, status=200)

        find_and_kill_command_server()

        assert_health_multiple_status(10, statuses={200, 502})

        wait_fail_window()

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
def test_multiple_get_request_to_three_servers_with_two_failed(request, configuration):
    pid = setup_ekilibri_server(request, f"tests/ekilibri-{configuration}.toml")
    try:
        assert_health(10, status=200)

        find_and_kill_command_server()

        assert_health_multiple_status(10, statuses={200, 502})

        wait_fail_window()

        assert_health(100, status=200)

        find_and_kill_command_server()

        assert_health_multiple_status(10, statuses={200, 502})

        wait_fail_window()

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
def test_multiple_get_request_to_three_servers_with_all_failed(request, configuration):
    pid = setup_ekilibri_server(request, f"tests/ekilibri-{configuration}.toml")
    try:
        assert_health(10, status=200)

        find_and_kill_all_command_servers()

        assert_health_multiple_status(10, statuses={502, 504})

        wait_fail_window()

        assert_health(10, status=502)
    finally:
        kill_process(pid)


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
def test_multiple_get_request_to_three_servers_with_all_failed_and_healthy_again(
    request, configuration, ports
):
    pid = setup_ekilibri_server(request, f"tests/ekilibri-{configuration}.toml")
    pids = []
    try:
        assert_health(10, status=200)

        find_and_kill_all_command_servers()

        assert_health_multiple_status(10, statuses={502, 504})

        wait_fail_window()

        assert_health(10, status=502)

        for port in ports:
            pids.append(setup_command_server(request, port))

        wait_fail_window()

        assert_health(100, status=200)
    finally:
        kill_process(pid)
        for pid in pids:
            kill_process(pid)
