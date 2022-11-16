# -*- coding: utf-8 -*-
# @Time    : 2022/7/13 16:35
# @Author  : Chris

import time
import logging
import datetime
import numpy as np
import pandas as pd
import pymongo
import statsmodels.api as sm
import matplotlib.pyplot as plt


######################################


# 定义计算中所需要的基本计算方法
class Basic_method(object):

    def __init__(self):
        pass

    # 极值处理方法一：中位数、分位点处理极值
    def winsorize_MAD(self, se, n):
        Median_ = np.median(se.dropna())
        MAD = (se - Median_).abs().median()
        edge_up = Median_ + n * MAD  # 中位数上n倍中位数均值
        edge_low = Median_ - n * MAD  # 中位数下n倍中位数均值
        edge_up_ = se.dropna().quantile(0.98)  # 98%分位点边界值
        edge_low_ = se.dropna().quantile(0.02)  # 2%分位点边界值
        se[se > edge_up_] = edge_up_
        se[se < edge_low_] = edge_low_
        se[se > edge_up] = edge_up
        se[se < edge_low] = edge_low
        return se

    # 极值处理方法二：标准差处理极值
    def winsorize(self, se, std):
        edge_up = se.mean() + std * se.std()
        edge_low = se.mean() - std * se.std()
        se[se > edge_up] = edge_up
        se[se < edge_low] = edge_low
        se[se == np.inf] = edge_up
        se[se == -np.inf] = edge_low
        return se

    # 将时间序列标准化处理
    def standardize(self, se):
        mean = se.mean()
        std = se.std()
        se = (se - mean) / std
        return se

    # 传入每日原始因子值，行业虚拟变量，风格因子值做回归中性化处理，返回当日中性化后的因子值（Seriers），数据格式均为：DataFrame
    def Factor_Neutralized(self, transactionFactor_Daily, industryFactors_Daily, barraStyleFactors_Daily):

        reg_Data = pd.concat([transactionFactor_Daily, industryFactors_Daily, barraStyleFactors_Daily], axis=1).replace(
            -np.inf, np.nan).replace(np.inf, np.nan).dropna()
        y_Var = np.array(reg_Data.iloc[:, 0])
        x_Var = sm.add_constant(np.array(reg_Data.iloc[:, 1:]))  # python包中OLS模型默认没有常数，需要同过此操作添加

        try:
            result = sm.OLS(y_Var, x_Var).fit()
        except ValueError:
            # print("%s gone error !" % symbol)
            return pd.Series([np.nan] * len(reg_Data), index=reg_Data.index)
            pass
        else:
            return pd.Series(result.resid, index=reg_Data.index)  # 返回回归beta, 回归残差的标准差



