import pytest

from ekilibri_setup import kill_command_server, setup_command_server


def pytest_addoption(parser):
    parser.addoption("--profile", action="store", default="local")
    parser.addoption("--setup-server", action="store", default="true")


# TODO: Initialize three different command servers and one ekilibri
# server.
@pytest.fixture(autouse=True)
def setup_and_teardown_rog_server(request):
    if request.config.getoption("--setup-server") == "true":
        pid = setup_command_server(request)
        yield
        try:
            kill_command_server(pid)
        except:
            # Ignores if it's not possible to kill the server, that
            # means the test has already killed it.
            pass
    else:
        yield
