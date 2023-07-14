import pandas as pd

from ai_transform.utils.json_encoder import json_encoder


class TestJSONEncoder:
    def test_json_encoder_timstamps(self):
        my_json_data = """[
{
    "itemId": "alpha:136:1",
    "testTime": 12.449,
    "workTime": 152.5,
    "project": "alpha",
    "user": "user100021-su7d",
    "timestamp": "170520161430",
    "accuracy": 1
},
{
    "itemId": "alpha:136:10",
    "testTime": 4.114,
    "workTime": 152.5,
    "project": "alpha",
    "user": "user100021-su7d",
    "timestamp": "170520161430",
    "accuracy": 0.8890000000000001
},
{
    "itemId": "alpha:136:100",
    "testTime": 5.114,
    "workTime": 43.4,
    "project": "alpha",
    "user": "user100021-su7d",
    "timestamp": "170522150338",
    "accuracy": 0.875
}
]
"""

        my_df = pd.read_json(my_json_data)
        json_encoder(my_df.to_dict("records"))
        assert True
