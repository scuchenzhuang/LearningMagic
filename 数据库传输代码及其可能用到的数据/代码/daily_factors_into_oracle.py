# -*- coding: utf-8 -*-
# @Time    : 2022/11/3 9:08
# @Author  : Chris

import pandas as pd
import numpy as np
import soupsieve
import statsmodels.api as sm
import matplotlib.pyplot as plt
import seaborn as sns
import statsmodels
import Get_daily_factor as FACTORS
import cx_Oracle
import time
import os
from datetime import datetime

#%% 汇研登录信息
tns = cx_Oracle.makedsn('10.243.73.69', 1521, 'orcl')
db = cx_Oracle.connect('QIHUO_CTP_APP', 'QIHUO_CTP_APP', tns)

#%% 定义特征入库函数
def update_feature_df(data_df):
    date_of_today = time.strftime("%Y-%m-%d", time.localtime())
    cursor = db.cursor()
    values = [x for x in data_df.apply(tuple, axis=1).tolist()]
    params = str(data_df.columns.to_list()).replace(str(data_df.columns.to_list())[1], '')[1:-1]
    value_column_num = data_df.columns.tolist().index('F_VALUE')
    select_list = ['F_DATE', 'F_VARITY', 'F_UPDATE_TYPE','F_TABLENAME','F_DATANAME']
    params_select = str(data_df[select_list].columns.to_list()).replace(str(data_df[select_list].columns.to_list())[1], '')[1:-1]
    count_0=0
    count_1=0
    for i in range(len(values)):
        # 查询
        quotes = str(values[0])[1]
        where_str = ''
        for i_select in range(len(select_list)):
            where_str = where_str + ' ' + data_df.columns[i_select] + '=' + quotes + values[i][i_select]+ quotes  + ' AND'
        where_str = where_str[:-4]
        sql = 'SELECT * FROM FACTOR_PAQHYJS_FEATURE WHERE' + where_str
        cursor.execute(sql)
        result = cursor.fetchall()

        if result == []:
            # 插入
            sql = 'INSERT into FACTOR_PAQHYJS_FEATURE(%s) values %s' % (params, str(values[i]))
            cursor.execute(sql)
            db.commit()
            print(values[i][0] + '插入')
        elif result[0][value_column_num] != values[i][value_column_num]:
            # 更新
            f_value = str(values[i][value_column_num])
            f_update = quotes+date_of_today+quotes
            sql = 'UPDATE FACTOR_PAQHYJS_FEATURE SET F_VALUE = %s, F_DATAREMARK = %s'%(f_value, f_update) + ' WHERE' + where_str
            cursor.execute(sql)
            db.commit()
            print(values[i][0] + '更新')
            count_0+=1
            if count_0>3:
                break
            else:
                continue
        else:
            # 未更新
            print(values[i][0] + '未更新')
            count_1+=1
            if count_1>3:
                break
            else:
                continue


#%% 定义因子入库函数
def update_factors_df(data_df):
    date_of_today = time.strftime("%Y-%m-%d", time.localtime())
    cursor = db.cursor()
    values = [x for x in data_df.apply(tuple, axis=1).tolist()]
    params = str(data_df.columns.to_list()).replace(str(data_df.columns.to_list())[1], '')[1:-1]
    value_column_num = data_df.columns.tolist().index('F_VALUE')
    select_list = ['F_DATE', 'F_DATANAME', 'F_TABLENAME']
    params_select = str(data_df[select_list].columns.to_list()).replace(str(data_df[select_list].columns.to_list())[1],'')[1:-1]
    k_count_0=0
    k_count_1=0
    for i in range(len(values)):
        # 查询
        quotes = str(values[0])[1]
        where_str = ''
        for i_select in range(len(select_list)):
            where_str = where_str + ' ' + data_df.columns[i_select] + '=' + quotes + values[i][i_select] + quotes + ' AND'
        where_str = where_str[:-4]
        sql = 'SELECT * FROM FACTOR_PAQHYJS_DATA WHERE' + where_str
        cursor.execute(sql)
        result = cursor.fetchall()

        if result == []:
            # 插入
            sql = 'INSERT into FACTOR_PAQHYJS_DATA(%s) values %s' % (params, str(values[i]))
            cursor.execute(sql)
            db.commit()
            print(values[i][0] + '插入')
        elif result[0][value_column_num] != values[i][value_column_num]:
            # 更新
            f_value = str(values[i][value_column_num])
            f_update = quotes + date_of_today + quotes
            sql = 'UPDATE FACTOR_PAQHYJS_DATA SET F_VALUE = %s, F_DATAREMARK = %s' % (f_value, f_update) + ' WHERE' + where_str
            cursor.execute(sql)
            db.commit()
            print(values[i][0] + '更新')
            k_count_0+=1
            if k_count_0>3:
                break
            else:
                continue
        else:
            # 未更新
            print(values[i][0] + '未更新')
            k_count_1+=1
            if k_count_1>3:
                break
            else:
                continue

#%% 定义插入或更新基本量价因子函数
def elementary_quant_factors_into_oracle_func():
    # %% 数据更新（每天一次）
    print('获取最新数据中')
    factor_data_update = FACTORS.Get_daily_factor()

    factor_data_update.update_data()

    factor_data=FACTORS.Get_daily_factor()

    print('最新数据获取完成，本轮品种基础量价特征更新开始')
    # %% 获取品种特征数据并且上传数据库
    # 获取品种其他信息（中文名、板块等）
    information_df = pd.read_excel('C:\\Users\\Administrator\\PycharmProjects\\平安期货code\\平安货期代码\\业绩归因\\code_info.xlsx', index_col=0)   ############！
    date_of_today = time.strftime("%Y-%m-%d", time.localtime())
    delete_list = ['PM', 'JR', 'RI', 'RS', 'WH', 'wr', 'fb', 'CY', 'bc', 'rr']
    code_list = information_df.index.tolist()
    for item in delete_list:
        try:
            code_list.remove(item)
        except:
            continue
    # 获取特征数据并且上传数据库
    factors_name_list = ['短期动量(5天)', '长期动量(20天)', '展期收益率', '波动率', '流动性','偏度']
    for i in range(len(factors_name_list)):
        print('正在处理的特征为：', factors_name_list[i])
        if i == 0:
            feature_data = factor_data.momentum(roll_window=5).sort_index(ascending=False).reindex(columns=code_list)
        elif i == 1:
            feature_data = factor_data.momentum(roll_window=20).sort_index(ascending=False).reindex(columns=code_list)
        elif i == 2:
            feature_data = factor_data.diff_ratio().sort_index(ascending=False).reindex(columns=code_list)
        elif i == 3:
            feature_data = factor_data.volatility().sort_index(ascending=False).reindex(columns=code_list)
        elif i == 4:
            feature_data = factor_data.liquidity_ILLIQ().sort_index(ascending=False).reindex(columns=code_list)
        else:
            feature_data = factor_data.skewness().sort_index(ascending=False).reindex(columns=code_list)
        for code in feature_data.columns.tolist():
            print('正在处理的品种为：', code)
            cat_feature_data = feature_data[[code]].dropna(axis=0)
            data_df = pd.DataFrame(
                columns=['F_DATE', 'F_VARITY', 'F_UPDATE_TYPE', 'F_TABLENAME', 'F_DATANAME', 'F_VALUE', 'F_UNIT', 'F_DATAREMARK'])
            data_df['F_DATE'] = [datetime.strftime(day, '%Y%m%d') for day in cat_feature_data.index.tolist()]
            data_df['F_VARITY'] = [code] * len(cat_feature_data.index.tolist())
            data_df['F_UPDATE_TYPE'] = [information_df.loc[code]['name']] * len(cat_feature_data.index.tolist())
            data_df['F_TABLENAME'] = [information_df.loc[code]['category']] * len(cat_feature_data.index.tolist())
            data_df['F_DATANAME'] = [factors_name_list[i]] * len(cat_feature_data.index.tolist())
            data_df['F_VALUE'] = cat_feature_data[code].values.tolist()
            data_df['F_UNIT'] = ['NAN'] * len(cat_feature_data.index.tolist())
            data_df['F_DATAREMARK'] = [date_of_today] * len(cat_feature_data.index.tolist())
            # print(data_df)
            update_feature_df(data_df)
    print('本轮品种基础量价特征更新结束')

    print('#####################################################################')

    print('本轮基础量价因子更新开始')
    # %% 获取因子值并且上传数据库
    # 获取因子数据
    factors_data_name_list = ['短期截面动量(5天)', '短期时序动量(5天)', '长期截面动量(20天)', '长期时序动量(20天)', '展期收益率', '波动率', '流动性','偏度']
    for i in range(len(factors_data_name_list)):
        print('正在处理的因子为：', factors_data_name_list[i])
        if i == 0:
            data_series = factor_data.factor_test(factor_df=factor_data.momentum(roll_window=5), show=False)[
                0].sort_index(ascending=False).dropna()
        elif i == 1:
            data_series = factor_data.time_series_momentum(show=False, rolling_window=5)[0].sort_index(
                ascending=False).dropna()
        elif i == 2:
            data_series = factor_data.factor_test(factor_df=factor_data.momentum(roll_window=20), show=False)[
                0].sort_index(ascending=False).dropna()
        elif i == 3:
            data_series = factor_data.time_series_momentum(show=False, rolling_window=20)[0].sort_index(
                ascending=False).dropna()
        elif i == 4:
            data_series = factor_data.factor_test(factor_df=factor_data.diff_ratio(), show=False)[0].sort_index(
                ascending=False).dropna()
        elif i == 5:
            data_series = factor_data.factor_test(factor_df=factor_data.volatility(), show=False)[0].sort_index(
                ascending=False).dropna()
        elif i == 6:
            data_series = factor_data.factor_test(factor_df=factor_data.liquidity_ILLIQ(), show=False)[0].sort_index(
                ascending=False).dropna()
        else:
            data_series = factor_data.factor_test(factor_df=factor_data.skewness(), show=False)[0].sort_index(
                ascending=False).dropna()
        factor_data_df = pd.DataFrame(columns=['F_DATE', 'F_DATANAME', 'F_TABLENAME', 'F_VALUE', 'F_DATAREMARK'])
        factor_data_df['F_DATE'] = [datetime.strftime(i, '%Y%m%d') for i in data_series.index.tolist()]
        factor_data_df['F_DATANAME'] = [factors_data_name_list[i]] * len(data_series)
        factor_data_df['F_TABLENAME'] = ['量价因子'] * len(data_series)
        factor_data_df['F_VALUE'] = data_series.values.tolist()
        factor_data_df['F_DATAREMARK'] = date_of_today
        # print(factor_data_df)
        update_factors_df(factor_data_df)
    print('本轮基础量价因子更新结束')


#%%调用主函数
if __name__=='__main__':
    print('日频量价特征和因子程序开始运行！')
    elementary_quant_factors_into_oracle_func()
    print('日频量价特征和因子程序结束运行！！')




















