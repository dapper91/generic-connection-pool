import random
from pathlib import Path

import pytest


@pytest.fixture
def test_dir() -> Path:
    return Path(__file__).parent


@pytest.fixture
def resource_dir(test_dir) -> Path:
    return test_dir / 'resources'


@pytest.fixture(autouse=True)
def init_random():
    random.seed(0)


@pytest.fixture(scope='session')
def sleep_delay(pytestconfig) -> float:
    return pytestconfig.getoption('--sleep-delay', 0.05)
