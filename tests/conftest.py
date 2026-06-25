import os
import pytest
from databricks.connect import DatabricksSession
"""
- We configure the spark session to be used across all tests, preventing the creation of diferent sessions for each test.
- If no env variable is provided the profile name will be vladicho (this because is the profile name in my case :P).
"""

@pytest.fixture(scope="session")
def spark():
    profile = os.getenv("DATABRICKS_CONFIG_PROFILE", "vladicho")
    session = (
        DatabricksSession.builder
        .profile(profile)
        .serverless(True)
        .getOrCreate()
    )
    yield session
    session.stop()