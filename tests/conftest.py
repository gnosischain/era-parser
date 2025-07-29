"""
Pytest configuration
"""
import pytest
from pathlib import Path


def pytest_configure(config):
    """Configure pytest"""
    config.addinivalue_line("markers", "slow: mark test as slow running")


@pytest.fixture(scope="session")
def test_data_dir():
    """Path to test data directory"""
    return Path(__file__).parent / "test_data"