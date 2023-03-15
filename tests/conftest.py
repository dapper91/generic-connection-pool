import random

import pytest


@pytest.fixture(autouse=True)
def init_random():
    random.seed(0)


@pytest.fixture(scope='session')
def sleep_delay(pytestconfig) -> float:
    return pytestconfig.getoption('--sleep-delay', 0.05)
