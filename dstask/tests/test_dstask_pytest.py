import os

from uuid import uuid4
from datetime import datetime
from fakeredis import FakeRedis
from unittest.mock import patch
from dstask.backend import RedisBackend
from pandas import DataFrame
from dstask.task import ds_task


@patch("dstask.backend.redis.Redis", return_value=FakeRedis())
def test_vanilla_with_redis(mock):
    backend = RedisBackend("localhost", 6379)
    expected_workflow_id = str(uuid4())
    expected_task_id = "12345"
    expected_date = datetime.today()
    os.environ["WORKFLOW_ID"] = expected_workflow_id
    os.environ["TASK_ID"] = expected_task_id

    @ds_task(backend=backend)
    def setup():
        return {
            "str_type": "e = mc^2",
            "df": DataFrame(),
            "dictionary_type": {"test": 1},
            "number": 9.81,
            "default_date": expected_date,
        }

    @ds_task(backend=backend)
    def test_workflow_and_task_id():
        for key in mock.return_value.scan_iter("*"):
            assert expected_workflow_id in str(key)
            assert expected_task_id in str(key)

    @ds_task(backend=backend)
    def test_data_pull(str_type, df, dictionary_type, number, default_date):
        assert str_type == "e = mc^2"
        assert df.equals(DataFrame())
        assert list(dictionary_type.keys()) == ["test"]
        assert dictionary_type["test"] == 1
        assert number == 9.81
        assert default_date == expected_date

    setup()
    test_workflow_and_task_id()
    test_data_pull()
