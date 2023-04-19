import random
from pathlib import Path
from typing import Generator

import pytest


@pytest.fixture(scope='session')
def test_dir() -> Path:
    return Path(__file__).parent


@pytest.fixture(scope='session')
def resource_dir(test_dir) -> Path:
    return test_dir / 'resources'


@pytest.fixture(autouse=True)
def init_random() -> None:
    random.seed(0)


@pytest.fixture(scope='session')
def delay(pytestconfig) -> float:
    return pytestconfig.getoption('--delay', 0.05)


@pytest.fixture(scope='session')
def port_gen() -> Generator[int, None, None]:
    def gen():
        for port in range(10000, 65535):
            yield port

    return gen()
