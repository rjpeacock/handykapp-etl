import sys
from pathlib import Path

import mongomock
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / 'src'))


@pytest.fixture
def mock_db():
    return mongomock.MongoClient().handykapp


@pytest.fixture(autouse=True)
def mock_logger(mocker):
    return mocker.patch("prefect.get_run_logger", return_value=mocker.MagicMock())
