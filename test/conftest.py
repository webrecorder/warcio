import pytest


def pytest_addoption(parser):
    parser.addoption(
        "--offline", action="store_true", help="Run tests in offline mode"
    )

def pytest_configure(config):
    config.addinivalue_line(
        "markers", "online: marks tests as requiring an internet connection"
    )

@pytest.fixture
def offline(request):
    return request.config.getoption("--offline")

@pytest.fixture(autouse=True)
def skip_if_offline(request, offline):
    if offline and request.node.get_closest_marker('online'):
        pytest.skip('requires internet connection')
