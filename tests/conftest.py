import pytest

from ekilibri_setup import kill_process, setup_command_server


def pytest_addoption(parser):
    parser.addoption("--profile", action="store", default="local")
    parser.addoption("--setup-server", action="store", default="true")


@pytest.fixture(autouse=True)
def setup_and_teardown_rog_server(request):
    if request.config.getoption("--setup-server") == "true":
        pids = []
        try:
            for port in (8081, 8082, 8083):
                pids.append(setup_command_server(request, port))
            yield
        finally:
            for pid in pids:
                try:
                    kill_process(pid)
                except:
                    # Ignores if it's not possible to kill the server, that
                    # means the test has already killed it.
                    pass
    else:
        yield
