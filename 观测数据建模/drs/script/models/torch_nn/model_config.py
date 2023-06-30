from feature_methods import *
from util import *

# Training params
# mode: train or predict
mode = 'train'
epoch = 1
batch_size = 256
shuffle = True
optimizer = 'adam'
learning_rate = 0.01
log_interval = 20
init_std = 0.001
emb_init_lower = -0.001
emb_init_upper = 0.001

# Data config
train_begin = '2023-03-31'
train_end = '2023-04-01'
eval_begin = '2023-04-02'
eval_end = '2023-06-26'
# eval_mode: day or week or all
eval_mode = 'week'
dt_col = 'dt'

# Feature config
# (feature_name, process_method, *process_args)
# self means transform feature inplace
preprocess_features = [
    ('log_user_cnt_s', map_interval, 'log_user_cnt', 100),
    ('log_subsidy_b_rate_s', map_interval, 'log_subsidy_b_rate', 100),
    ('bubble_cnt_pp_s', map_interval, 'bubble_cnt_pp', 100),
    ('compete_call_cnt_pp_s', map_interval, 'compete_call_cnt_pp', 100),
    ('objective_call_cnt_pp_s', map_interval, 'objective_call_cnt_pp', 100),
    ('ans_cnt_pp_s', map_interval, 'ans_cnt_pp', 100),
    ('suc_cnt_pp_s', map_interval, 'suc_cnt_pp', 100),
    ('business_profit_rate_s', map_interval, 'business_profit_rate', 100),
    ('online_driver_divide_rate_s', map_interval, 'online_driver_divide_rate', 100),
    ('order_charge_dis_pp_s', map_interval, 'order_charge_dis_pp', 100),
    ('driver_charge_dis_pp_s', map_interval, 'driver_charge_dis_pp', 100),
    ('log_user_cnt', standardize, 'self'),
    ('log_subsidy_b_rate', standardize, 'self'),
    ('bubble_cnt_pp', standardize, 'self'),
    ('compete_call_cnt_pp', standardize, 'self'),
    ('objective_call_cnt_pp', standardize, 'self'),
    ('ans_cnt_pp', standardize, 'self'),
    ('suc_cnt_pp', standardize, 'self'),
    ('business_profit_rate', standardize, 'self'),
    ('online_driver_divide_rate', standardize, 'self'),
    ('order_charge_dis_pp', standardize, 'self'),
    ('driver_charge_dis_pp', standardize, 'self')
]
preprocess_features = get_preprocess_conf(preprocess_features)
# (feature_name, embedding_dimension, num_embeddings, embedding_name)
# if embedding_name is omitted, use feature_name instead
sparse_features = [
    ('city_id', 2, 1000),
    ('log_user_cnt_s', 1, 1000),
    ('log_subsidy_b_rate_s', 1, 1000),
    ('bubble_cnt_pp_s', 1, 1000),
    ('compete_call_cnt_pp_s', 1, 1000),
    ('objective_call_cnt_pp_s', 1, 1000),
    ('ans_cnt_pp_s', 1, 1000),
    ('suc_cnt_pp_s', 1, 1000),
    ('business_profit_rate_s', 1, 1000),
    ('online_driver_divide_rate_s', 1, 1000),
    ('order_charge_dis_pp_s', 1, 1000),
    ('driver_charge_dis_pp_s', 1, 1000),
]
dense_features = [
    'log_user_cnt',
    'log_subsidy_b_rate',
    'bubble_cnt_pp',
    'compete_call_cnt_pp',
    'objective_call_cnt_pp',
    'ans_cnt_pp',
    'suc_cnt_pp',
    'business_profit_rate',
    'online_driver_divide_rate',
    'order_charge_dis_pp',
    'driver_charge_dis_pp'
]
treatments = [
    'subsidy_c_rate',
]
labels = [
    'gmv_pp',
]
all_features = treatments + dense_features + [v[0] for v in sparse_features]
all_sparse = get_sparse_conf(sparse_features)
all_dense = treatments + dense_features
