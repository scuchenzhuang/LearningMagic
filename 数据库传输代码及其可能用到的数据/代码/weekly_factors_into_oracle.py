# -*- coding: utf-8 -*-
# @Time    : 2022/11/1 17:04
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
from datetime import timedelta
import calendar

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

#%% 定义读取基本面数据函数
def get_fundamental_InventoryData_oracle(info_df,input_info,del_list):
    '''
    :param info_df:   输入交易所的映射表
    :param input_info:  输入汇研的映射表
    :return:  输出存放着各个品种基差的字典
    '''
    result_dict={}
    cursor = db.cursor()  # 创建数据库游标
    info_df=info_df.copy()
    input_info=input_info.copy()
    require_factors_name=['库存']                   #['利润','基差','库存']
    for i in input_info.keys():
        if i in require_factors_name:
            res_df=input_info[i][['f_code','data_name','db_table']].set_index('f_code')
            code_list=list(np.sort(list(set(info_df.index.tolist()).intersection(set(res_df.index.tolist())))))
            for item in code_list:
                if item in del_list:
                    code_list.remove(item)
                else:
                    continue
            select_info_df=res_df.reindex(index=code_list)
            for code_name in select_info_df.index.tolist():
                table_name=select_info_df.loc[code_name]['db_table'].upper()
                sql = 'SELECT * FROM ' + table_name  # sql语句
                cursor.execute(sql)  # 执行语句
                result = cursor.fetchall()  # 获取数据
                col_list = [col[0] for col in cursor.description]  # 获取字段列表
                data_df = pd.DataFrame(result, columns=col_list)  # 整理成dataframe格式
                data_df02=pd.DataFrame(columns=[code_name])
                data_df02[code_name]=data_df[data_df['F_DATANAME']==select_info_df.loc[code_name]['data_name']][['F_DATE', 'F_VALUE']].set_index('F_DATE').sort_index()['F_VALUE'].apply(float)
                data_df02.index.name='date'
                result_dict[code_name]=data_df02
        else:
            continue
    return result_dict


#%% 定义日期处理函数(非滞后类别)
def deal_datetime_func(df):
    data_df = df.copy()
    datetime_list=[]
    for insight_original_date in data_df.index.tolist():
        insight_date=pd.Timestamp(insight_original_date)
        if calendar.weekday(year=insight_date.year, month=insight_date.month, day=insight_date.day) == 3:
            time=insight_date+timedelta(days=1)
            datetime_list.append(time)
        elif calendar.weekday(year=insight_date.year, month=insight_date.month, day=insight_date.day) == 2:
            time = insight_date + timedelta(days=2)
            datetime_list.append(time)
        elif calendar.weekday(year=insight_date.year, month=insight_date.month, day=insight_date.day) == 1:
            time = insight_date + timedelta(days=3)
            datetime_list.append(time)
        elif calendar.weekday(year=insight_date.year, month=insight_date.month, day=insight_date.day) == 0:
            time = insight_date + timedelta(days=4)
            datetime_list.append(time)
        elif calendar.weekday(year=insight_date.year, month=insight_date.month, day=insight_date.day) == 5:
            time = insight_date + timedelta(days=-1)
            datetime_list.append(time)
        elif calendar.weekday(year=insight_date.year, month=insight_date.month, day=insight_date.day) == 6:
            time = insight_date + timedelta(days=-2)
            datetime_list.append(time)
        else:
            datetime_list.append(insight_date)

    data_df.index=[datetime.strftime(i,'%Y%m%d') for i in datetime_list]
    data_df = data_df[~data_df.index.duplicated()]
    data_df.index.name='date'
    return data_df

#%% 定义日期处理函数(滞后类别)
def deal_lag_datetime_func(df):
    data_df = df.copy()
    datetime_list=[]
    for insight_original_date in data_df.index.tolist():
        insight_date=pd.Timestamp(insight_original_date)
        if calendar.weekday(year=insight_date.year, month=insight_date.month, day=insight_date.day) == 3:
            time=insight_date+timedelta(days=-6)
            datetime_list.append(time)
        elif calendar.weekday(year=insight_date.year, month=insight_date.month, day=insight_date.day) == 2:
            time = insight_date + timedelta(days=-5)
            datetime_list.append(time)
        elif calendar.weekday(year=insight_date.year, month=insight_date.month, day=insight_date.day) == 1:
            time = insight_date + timedelta(days=-4)
            datetime_list.append(time)
        elif calendar.weekday(year=insight_date.year, month=insight_date.month, day=insight_date.day) == 0:
            time = insight_date + timedelta(days=-3)
            datetime_list.append(time)
        else:
            datetime_list.append(insight_date)

    data_df.index=[datetime.strftime(i,'%Y%m%d') for i in datetime_list]
    data_df = data_df[~data_df.index.duplicated()]
    data_df.index.name='date'
    return data_df


#%% 定义计算库存因子函数
def cal_inventory_factor_func(dict,special_item_list):
    # 构造工具
    tool_list = []
    time_list = [datetime.strftime(i, '%Y%m%d') for i in pd.date_range(start='20100516', end=datetime.now(),freq='W-FRI')]
    tool_df = pd.DataFrame(['NAN'] * len(time_list), index=time_list, columns=['tool'])
    tool_list.append(tool_df)
    # 处理全部数据日期
    for code in dict.keys():
        if code in special_item_list:
            need_data_df=deal_lag_datetime_func(dict[code])
        else:
            need_data_df=deal_datetime_func(dict[code])
        rank_df = need_data_df.rank(ascending=False) / len(need_data_df.index.tolist())
        rank_df = rank_df.loc[~rank_df.index.duplicated(keep='first')]
        tool_list.append(rank_df)
    factor_df = pd.concat(tool_list, axis=1)
    factor_data_df = factor_df.drop(columns=['tool']).dropna(axis=0, how='all').dropna(thresh=2).sort_index()
    factor_data_df.index=[pd.Timestamp(i) for i in factor_data_df.index.tolist()]

    open_data = FACTORS.Get_daily_factor().get_main_contract_open_all()
    open_data.index=[pd.Timestamp(i) for i in open_data.index.tolist()]

    factor_data_df=factor_data_df.loc[open_data.index.tolist()[0]:]

    open_all_ret = pd.DataFrame(index=open_data.index.tolist(),columns=open_data.columns.tolist())

    open_date_end_date=factor_data_df.index.tolist()[0]
    id_name_list=[]
    for original_date in factor_data_df.index.tolist():
        last_factors_date = original_date
        if np.max([last_factors_date,open_date_end_date])==last_factors_date:
            date=last_factors_date
        elif (np.max([last_factors_date,open_date_end_date])==open_date_end_date)and(open_date_end_date!=last_factors_date):
            continue
        else:
            date=original_date
        try:
            if open_data.loc[date:].index.tolist()[0] == date:
                open_data_start = open_data.loc[date:].iloc[1, :]
                try:
                    open_data_end = open_data.loc[date:].iloc[6, :]
                except:
                    open_data_end = open_data.loc[date:].iloc[-1, :]
                    break
            else:
                open_data_start = open_data.loc[date:].iloc[0, :]
                try:
                    open_data_end = open_data.loc[date:].iloc[5, :]
                except:
                    open_data_end = open_data.loc[date:].iloc[-1, :]
                    break
        except:
            break
        ret = (open_data_end - open_data_start) / open_data_start
        open_all_ret.loc[open_data_end.name]=ret
        open_date_end_date=open_data_end.name
        id_name_list.append(date)

    open_all_ret=open_all_ret.dropna(thresh=2).sort_index()

    open_all_ret.index=[datetime.strftime(i,'%Y%m%d') for i in open_all_ret.index.tolist()]
    open_all_ret.index.name='date'
    factor_data_df.index=[datetime.strftime(i,'%Y%m%d') for i in factor_data_df.index.tolist()]
    factor_data_df.index.name='date'

    factor_ret = pd.Series(index=open_all_ret.index.tolist())

    id_name_list=[datetime.strftime(i,'%Y%m%d') for i in id_name_list]
    for id in range(len(id_name_list)):
        factor_sort = factor_data_df.loc[id_name_list[id]].sort_values().dropna()
        symbol_num_20pct = round(len(factor_sort) * 0.2)
        factor_ret.iloc[id] = open_all_ret.iloc[id][factor_sort.iloc[-symbol_num_20pct:].index].mean() - \
                               open_all_ret.iloc[id][factor_sort.iloc[:symbol_num_20pct].index].mean()
    factor_ret=factor_ret.fillna(method='ffill').fillna(method='bfill')
    return factor_ret

#%% 定义库存特征和因子入库和更新函数
def inventory_factor_into_oracle_func():
    # %% 更新各品种库存特征
    print('开始插入或者更新各品种库存特征')
    date_of_today = time.strftime("%Y-%m-%d", time.localtime())
    code_info = pd.read_excel('C:\\Users\\Administrator\\PycharmProjects\\平安期货code\\平安货期代码\\业绩归因\\code_info.xlsx', index_col=0)   ########!
    res_info = pd.read_excel('C:\\Users\\Administrator\\PycharmProjects\\平安期货code\\平安货期代码\\数据库\\库存-利润-基差-汇总.xlsx', sheet_name=None)  ######!
    del_category_list = ['sp', 'ss', 'SR', 'lh']
    feature_dict = get_fundamental_InventoryData_oracle(info_df=code_info, input_info=res_info,
                                                        del_list=del_category_list)
    for code in feature_dict.keys():
        print(code)
        data_df = pd.DataFrame(
            columns=['F_DATE', 'F_VARITY', 'F_UPDATE_TYPE', 'F_TABLENAME', 'F_DATANAME', 'F_VALUE', 'F_UNIT', 'F_DATAREMARK'])
        data_df['F_DATE'] = feature_dict[code].sort_index(ascending=False).index.tolist()
        data_df['F_VARITY'] = [code] * len(feature_dict[code].sort_index(ascending=False).index.tolist())
        data_df['F_UPDATE_TYPE'] = [code_info.loc[code]['name']] * len(
            feature_dict[code].sort_index(ascending=False).index.tolist())
        data_df['F_TABLENAME'] = [code_info.loc[code]['category']] * len(
            feature_dict[code].sort_index(ascending=False).index.tolist())
        data_df['F_DATANAME'] = ['库存数值'] * len(feature_dict[code].sort_index(ascending=False).index.tolist())
        data_df['F_VALUE'] = feature_dict[code].sort_index(ascending=False)[code].values.tolist()
        data_df['F_UNIT'] = [res_info['库存'][['f_code', 'unit']].set_index('f_code').loc[code]['unit']] * len(
            feature_dict[code].sort_index(ascending=False).index.tolist())
        data_df['F_DATAREMARK'] = [date_of_today] * len(feature_dict[code].sort_index(ascending=False).index.tolist())
        update_feature_df(data_df)
    print('各品种库存特征插入或者更新结束')

    special_items_list = ['m', 'y', 'p', 'RM', 'OI']

    # %%计算库存因子
    print('开始计算库存因子')
    inventory_factor_ret = cal_inventory_factor_func(dict=feature_dict, special_item_list=special_items_list)
    inventory_factor_ret = inventory_factor_ret.sort_index(ascending=False)
    print('结束计算库存因子')
    # %% 更新利润因子数值
    print('开始插入或者更新库存因子')
    inventory_factor_data_df = pd.DataFrame(
        columns=['F_DATE', 'F_DATANAME', 'F_TABLENAME', 'F_VALUE', 'F_DATAREMARK'])
    inventory_factor_data_df['F_DATE'] = inventory_factor_ret.index.tolist()
    inventory_factor_data_df['F_DATANAME'] = ['库存因子'] * len(inventory_factor_ret.index.tolist())
    inventory_factor_data_df['F_TABLENAME'] = ['基本面因子'] * len(inventory_factor_ret.index.tolist())
    inventory_factor_data_df['F_VALUE'] = inventory_factor_ret.values.tolist()
    inventory_factor_data_df['F_DATAREMARK'] = [date_of_today] * len(inventory_factor_ret.index.tolist())
    update_factors_df(inventory_factor_data_df)
    print('插入或者更新库存因子结束')



#%% 调用主函数
if __name__=='__main__':
    print('周频基本面特征和因子程序开始运行！')
    inventory_factor_into_oracle_func()
    print('周频基本面特征和因子程序结束运行！！')






