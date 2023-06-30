import pandas as pd

PARAM = {
    "step_start_date": "2022-03-30",
    "rely_info": {
        "gmv_code": {
            "gmv_dt": "2022-03-29",
            "cr_threshold": 0.72
        }
    },
    "budget_limit": 700000,
    "ext_data": {
        "input_city_info": [
            {
                "date": '2022-03-30',
                "city_id": 11,
                "product_line": "kuaiche",
                "caller": "b"
            },
            {
                "date": '2022-03-30',
                "city_id": 17,
                "product_line": "kuaiche",
                "caller": "b"
            },
            {
                "date": '2022-03-30',
                "city_id": 18,
                "product_line": "kuaiche",
                "caller": "b"
            },
            {
                "date": '2022-03-30',
                "city_id": 8,
                "product_line": "kuaiche",
                "caller": "b"
            }
        ],
        "artificial": [
            {
                "date": '2022-03-30',
                "city_id": 3,
                "time_rage": ['07:00:00', '09:00:00'],
                "fence_id":-1,
                "caller": 'b',
                "daily_b_rate": 0.05
            },
            {
                "date": '2022-03-30',
                "city_id": 28,
                "time_rage": ['17:00:00', '20:00:00'],
                "fence_id": -1,
                "caller": 'b',
                "daily_b_rate": 0.08
            }
        ],
        "budget_limit": [
            {
                "date": '2022-03-30',
                "city_id": 11,
                "daily_b_rate_limit": 0.04
            },
            {
                "date": '2022-03-30',
                "city_id": 17,
                "daily_b_rate_limit": 0.10
            }
        ]
    }
}

ARGUMENT = {
    'valid_cities': [11, 17, 18, 8],
    'start_date': '2022-03-30',
    'gmv_dt': '2022-03-29',
    'strategy_type': 'fixed_threshold_72',
    'budget': 700000,
    'cr_threshold': 0.72,
    'artificial_startegy': pd.DataFrame([
        {
            'dt': '2022-03-30',
            'city_id': 3,
            'fence_id': -1,
            'start_time': '07:00:00',
            'end_time': '09:00:00',
            'daily_b_rate': 0.05
        },
        {
            'dt': '2022-03-30',
            'city_id': 28,
            'fence_id': -1,
            'start_time': '17:00:00',
            'end_time': '20:00:00',
            'daily_b_rate': 0.08
        }
    ]),
    'b_ratio_limit': pd.DataFrame([
        {
            'date': '2022-03-30',
            'city_id': 11,
            'daily_b_rate_limit': 0.04
        },
        {
            'date': '2022-03-30',
            'city_id': 17,
            'daily_b_rate_limit': 0.1
        }
    ]),
}
