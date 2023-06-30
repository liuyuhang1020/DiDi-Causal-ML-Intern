"""项目配置和常量"""
from enum import IntEnum


# Consts
# 分配模式
class Mode(IntEnum):
    B = 0  # B 端分配
    D = 1  # 调度分配
    C = 3  #  C 端分配


# 品类
class C(IntEnum):
    fk = 0  # 泛快：特惠自营+普通快车
    zc = 1  # 专车
    czc = 2  # 出租车


class B(IntEnum):
    fk = 0  # 泛快：特惠自营+普通快车
    zc = 1  # 专车
    czc = 2  # 出租车


Y_M_D = '%Y-%m-%d'
YMD = '%Y%m%d'
Valid_Col = 'is_candidate'
max_try_num = 7
MIN_NUM_OF_DATA = 0
