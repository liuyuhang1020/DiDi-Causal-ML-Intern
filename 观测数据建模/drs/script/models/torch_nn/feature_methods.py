import sklearn.preprocessing as pre
import numpy as np


def min_max_norm(df, col):
    return pre.minmax_scale(df[col])

def standardize(df, col):
    return pre.scale(df[col])

def multiply(df, col, scale):
    out = df[col] * scale
    return out.round(0)

def clip_log(df, col, lower, upper, scale1, scale2):
    out = df[col].clip(lower, upper)
    out = np.log(out / scale1 + 1) * scale2
    return out.round(0)

def map_interval(df, col, upper):
    out = upper*(df[col] - df[col].min())/(df[col].max() - df[col].min())
    return out.round(0)