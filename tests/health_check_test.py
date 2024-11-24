import pytest

from common import assert_health, wait_fail_window
from ekilibri_setup import kill_process, setup_ekilibri_server


@pytest.mark.parametrize(
    "configuration",
    [
        pytest.param("least-connections", marks=pytest.mark.least_connections),
        pytest.param("round-robin", marks=pytest.mark.round_robin),
    ],
)
def test_get_requests_when_the_health_check_path_is_timing_out(request, configuration):
    pid = setup_ekilibri_server(request, f"tests/ekilibri-{configuration}-timeout.toml")
    try:
        # First requests should be ok, given that Ekilibri still hasn't removed
        # the servers from healthy servers list.
        assert_health(100, status=200)

        wait_fail_window()

        assert_health(10, status=502)
    finally:
        kill_process(pid)
