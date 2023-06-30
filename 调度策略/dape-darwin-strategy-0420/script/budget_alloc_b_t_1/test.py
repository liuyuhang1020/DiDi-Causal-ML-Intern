import unittest
import json
from test_sample import PARAM, ARGUMENT
from main import get_argument_parse, get_task, t_plus_1_alloc_tasks, validate_tasks, generate_final_tasks
import pandas as pd
from pandas.testing import assert_frame_equal
from unittest.mock import patch


class TestBudgetAlloc(unittest.TestCase):

    def setUp(self):
        """

        :return:
        """
        self.param = json.dumps(PARAM)

    def test_get_argument_parse(self):
        param = get_argument_parse(self.param)
        for key, val in param.items():
            if isinstance(val, pd.DataFrame):
                self.assertIsNone(assert_frame_equal(param[key], ARGUMENT[key]))
            else:
                self.assertEqual(param[key], ARGUMENT[key])

    @patch("requests.post")
    def test_get_task(self, mock_post):
        day_gmv = {
            "data": [
                {
                    "data": [
                        {
                            "date": "2022-03-30",
                            "st": "2022-03-30 00:00:00",
                            "value": 5000000
                        }
                    ],
                    "city_id": 3
                },
                {
                    "data": [
                        {
                            "date": "2022-03-30",
                            "st": "2022-03-30 00:00:00",
                            "value": 4500000
                        }
                    ],
                    "city_id": 11
                },
                {
                    "data": [
                        {
                            "date": "2022-03-30",
                            "st": "2022-03-30 00:00:00",
                            "value": 4000000
                        }
                    ],
                    "city_id": 28
                },
                {
                    "data": [
                        {
                            "date": "2022-03-30",
                            "st": "2022-03-30 00:00:00",
                            "value": 3500000
                        }
                    ],
                    "city_id": 17
                },
                {
                    "data": [
                        {
                            "date": "2022-03-30",
                            "st": "2022-03-30 00:00:00",
                            "value": 3000000
                        }
                    ],
                    "city_id": 18
                },
                {
                    "data": [
                        {
                            "date": "2022-03-30",
                            "st": "2022-03-30 00:00:00",
                            "value": 6000000
                        }
                    ],
                    "city_id": 8
                }
            ]
        }
        hour_gmv = {
            "data": [
                {
                    "data": [
                        {
                            "date": "2022-03-30",
                            "st": "2022-03-30 07:00:00",
                            "value": 250000
                        },
                        {
                            "date": "2022-03-30",
                            "st": "2022-03-30 08:00:00",
                            "value": 300000
                        },
                        {
                            "date": "2022-03-30",
                            "st": "2022-03-30 20:00:00",
                            "value": 250000
                        },
                        {
                            "date": "2022-03-30",
                            "st": "2022-03-30 17:00:00",
                            "value": 220000
                        },
                        {
                            "date": "2022-03-30",
                            "st": "2022-03-30 18:00:00",
                            "value": 250000
                        },
                        {
                            "date": "2022-03-30",
                            "st": "2022-03-30 19:00:00",
                            "value": 250000
                        }
                    ],
                    "city_id": 3
                },
                {
                    "data": [
                        {
                            "date": "2022-03-30",
                            "st": "2022-03-30 07:00:00",
                            "value": 230000
                        },
                        {
                            "date":"2022-03-30",
                            "st":"2022-03-30 08:00:00",
                            "value":270000
                        },
                        {
                            "date":"2022-03-30",
                            "st":"2022-03-30 20:00:00",
                            "value":230000
                        },
                        {
                            "date":"2022-03-30",
                            "st":"2022-03-30 17:00:00",
                            "value":200000
                        },
                        {
                            "date":"2022-03-30",
                            "st":"2022-03-30 18:00:00",
                            "value":230000
                        },
                        {
                            "date":"2022-03-30",
                            "st":"2022-03-30 19:00:00",
                            "value":230000
                        }
                    ],
                    "city_id": 11
                },
                {
                    "data": [
                        {
                            "date":"2022-03-30",
                            "st":"2022-03-30 07:00:00",
                            "value":200000
                        },
                        {
                            "date":"2022-03-30",
                            "st":"2022-03-30 08:00:00",
                            "value":240000
                        },
                        {
                            "date":"2022-03-30",
                            "st":"2022-03-30 20:00:00",
                            "value":2000000
                        },
                        {
                            "date":"2022-03-30",
                            "st":"2022-03-30 17:00:00",
                            "value":180000
                        },
                        {
                            "date":"2022-03-30",
                            "st":"2022-03-30 18:00:00",
                            "value":200000
                        },
                        {
                            "date":"2022-03-30",
                            "st":"2022-03-30 19:00:00",
                            "value":200000
                        },
                        {
                            "date": "2022-03-30",
                            "st": "2022-03-30 16:00:00",
                            "value": 200000
                        }
                    ],
                    "city_id": 28
                },
                {
                    "data": [
                        {
                            "date": "2022-03-30",
                            "st": "2022-03-30 07:00:00",
                            "value": 170000
                        },
                        {
                            "date":"2022-03-30",
                            "st":"2022-03-30 08:00:00",
                            "value":190000
                        },
                        {
                            "date":"2022-03-30",
                            "st":"2022-03-30 20:00:00",
                            "value":170000
                        },
                        {
                            "date":"2022-03-30",
                            "st":"2022-03-30 21:00:00",
                            "value":150000
                        },
                        {
                            "date":"2022-03-30",
                            "st":"2022-03-30 18:00:00",
                            "value":170000
                        },
                        {
                            "date":"2022-03-30",
                            "st":"2022-03-30 19:00:00",
                            "value":170000
                        }
                    ],
                    "city_id": 17
                },
                {
                    "data": [
                        {
                            "date": "2022-03-30",
                            "st": "2022-03-30 07:00:00",
                            "value": 150000
                        },
                        {
                            "date": "2022-03-30",
                            "st": "2022-03-30 08:00:00",
                            "value": 170000
                        },
                        {
                            "date": "2022-03-30",
                            "st": "2022-03-30 20:00:00",
                            "value": 150000
                        },
                        {
                            "date": "2022-03-30",
                            "st": "2022-03-30 17:00:00",
                            "value": 140000
                        },
                        {
                            "date": "2022-03-30",
                            "st": "2022-03-30 18:00:00",
                            "value": 150000
                        },
                        {
                            "date": "2022-03-30",
                            "st": "2022-03-30 19:00:00",
                            "value": 150000
                        }
                    ],
                    "city_id": 18
                },
                {
                    "data": [
                        {
                            "date": "2022-03-30",
                            "st": "2022-03-30 07:00:00",
                            "value": 300000
                        },
                        {
                            "date": "2022-03-30",
                            "st": "2022-03-30 08:00:00",
                            "value": 350000
                        },
                        {
                            "date": "2022-03-30",
                            "st": "2022-03-30 20:00:00",
                            "value": 300000
                        },
                        {
                            "date": "2022-03-30",
                            "st": "2022-03-30 23:00:00",
                            "value": 240000
                        },
                        {
                            "date": "2022-03-30",
                            "st": "2022-03-30 22:00:00",
                            "value": 300000
                        },
                        {
                            "date": "2022-03-30",
                            "st": "2022-03-30 21:00:00",
                            "value": 300000
                        }
                    ],
                    "city_id": 8
                }
            ]
        }
        mock_post.side_effect = [
            day_gmv, hour_gmv, day_gmv, hour_gmv, day_gmv, hour_gmv
        ]

        raw_tasks, art_tasks, budget_limit = get_task(ARGUMENT)
        print("have done， get tasks")
        print(raw_tasks)
        print(art_tasks)
        print(budget_limit)
        budget_limit[11] = 50000
        ARGUMENT['latest_budget'] = ARGUMENT['budget'] - art_tasks['budget'].sum()
        tasks = t_plus_1_alloc_tasks(ARGUMENT, raw_tasks, budget_limit)
        print(tasks)
        cols = ['dt', 'city_id', 'fence_id', 'start_time', 'end_time', "cr", "cr_thres", "gmv", "budget", "total_gmv"]
        tasks = pd.concat([tasks[cols], art_tasks[cols]])
        err = validate_tasks(tasks)
        if err != "":
            raise Exception(f"预算格式非法：{err}")
        # 返回结果
        ret_data = generate_final_tasks(ARGUMENT, tasks)
        print(ret_data)


if __name__ == '__main__':
    unittest.main()
