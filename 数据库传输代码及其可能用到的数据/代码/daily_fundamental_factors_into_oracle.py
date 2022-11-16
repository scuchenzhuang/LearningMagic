# -*- coding: utf-8 -*-
# @Time    : 2022/11/1 16:48
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


#%% 定义读取基差基本面数据函数
def get_fundamental_basis_data_oracle(info_df,input_info):
    '''
    :param info_df:   输入交易所的映射表
    :param input_info:  输入汇研的映射表
    :return:  输出存放着各个品种基差的字典
    '''
    result_dict={}
    cursor = db.cursor()  # 创建数据库游标
    info_df=info_df.copy()
    input_info=input_info.copy()
    require_factors_name=['基差']                   #['利润','基差','库存']
    for i in input_info.keys():
        if i in require_factors_name:
            res_df=input_info[i][['f_code','data_name','db_table']].set_index('f_code')
            code_list=list(np.sort(list(set(info_df.index.tolist()).intersection(set(res_df.index.tolist())))))
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

#%% 定义计算基差因子函数
def cal_basis_factor_func(dict):
    # 构造工具
    tool_list=[]
    time_list=[datetime.strftime(i,'%Y%m%d') for i in pd.date_range(start='20100516',end=datetime.now())]
    tool_df=pd.DataFrame(['NAN']*len(time_list),index=time_list,columns=['tool'])
    tool_list.append(tool_df)
    # 计算因子值
    for code in dict.keys():
        rank_df=dict[code].rank()/len(dict[code].index.tolist())
        rank_df=rank_df.loc[~rank_df.index.duplicated(keep='first')]
        tool_list.append(rank_df)
    factor_df=pd.concat(tool_list,axis=1)
    factor_data_df=factor_df.drop(columns=['tool']).dropna(axis=0,how='all').dropna(thresh=2).sort_index()

    open_ret_data = FACTORS.Get_daily_factor().daily_ret_all_open
    open_ret_data.index=[datetime.strftime(i,'%Y%m%d') for i in open_ret_data.index.tolist()]
    open_ret_data=open_ret_data.reindex(index=factor_data_df.index.tolist()).dropna(axis=0,how='all')

    factor_ret = pd.Series(index=open_ret_data.index.tolist())
    for date in factor_ret.index.tolist():
        factor_sort = factor_data_df.shift(1).loc[date].sort_values().dropna()
        symbol_num_20pct = round(len(factor_sort) * 0.2)
        factor_ret.loc[date] = open_ret_data.loc[date, factor_sort.iloc[-symbol_num_20pct:].index].mean() - \
                               open_ret_data.loc[date, factor_sort.iloc[:symbol_num_20pct].index].mean()
    factor_ret=factor_ret.fillna(method='ffill').fillna(method='bfill')
    return factor_ret

#%% 定义修改数据库数据函数(基差)
def modified_table_feature_basis_func():
    cursor = db.cursor()
    mod_sql="""UPDATE FACTOR_PAQHYJS_FEATURE  
SET F_UNIT = '元/500千克'  
WHERE F_VARITY = 'jd' AND F_DATANAME='基差数值'
    """
    cursor.execute(mod_sql)

#%% 定义基差特征和因子入库或更新函数
def basis_factor_into_oracle_func():
    # %% 更新各品种基差特征
    print('开始插入或者更新各品种基差特征')
    date_of_today = time.strftime("%Y-%m-%d", time.localtime())
    code_info = pd.read_excel('C:\\Users\\Administrator\\PycharmProjects\\平安期货code\\平安货期代码\\业绩归因\\code_info.xlsx', index_col=0)  ########!
    res_info = pd.read_excel('C:\\Users\\Administrator\\PycharmProjects\\平安期货code\\平安货期代码\\数据库\\库存-利润-基差-汇总.xlsx', sheet_name=None)   ########!
    feature_dict = get_fundamental_basis_data_oracle(info_df=code_info, input_info=res_info)

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
        data_df['F_DATANAME'] = ['基差数值'] * len(feature_dict[code].sort_index(ascending=False).index.tolist())
        data_df['F_VALUE'] = feature_dict[code].sort_index(ascending=False)[code].values.tolist()
        data_df['F_UNIT'] = ['元/吨'] * len(feature_dict[code].sort_index(ascending=False).index.tolist())
        data_df['F_DATAREMARK'] = [date_of_today] * len(feature_dict[code].sort_index(ascending=False).index.tolist())
        update_feature_df(data_df)
    print('各品种基差特征插入或者更新结束')

    # %% 计算基差因子
    print('开始计算基差因子')
    basis_factor_ret = cal_basis_factor_func(dict=feature_dict)
    basis_factor_ret = basis_factor_ret.sort_index(ascending=False)
    print('结束计算基差因子')
    # %% 更新基差因子数值
    print('开始插入或者更新基差因子')
    basis_factor_data_df = pd.DataFrame(columns=['F_DATE', 'F_DATANAME', 'F_TABLENAME', 'F_VALUE', 'F_DATAREMARK'])
    basis_factor_data_df['F_DATE'] = basis_factor_ret.index.tolist()
    basis_factor_data_df['F_DATANAME'] = ['基差因子'] * len(basis_factor_ret.index.tolist())
    basis_factor_data_df['F_TABLENAME'] = ['基本面因子'] * len(basis_factor_ret.index.tolist())
    basis_factor_data_df['F_VALUE'] = basis_factor_ret.values.tolist()
    basis_factor_data_df['F_DATAREMARK'] = [date_of_today] * len(basis_factor_ret.index.tolist())
    update_factors_df(basis_factor_data_df)
    modified_table_feature_basis_func()
    print('插入或者更新基差因子结束')


#%% 定义读取基本面数据函数
def get_fundamental_profit_data_oracle(info_df,input_info):
    '''
    :param info_df:   输入交易所的映射表
    :param input_info:  输入汇研的映射表
    :return:  输出存放着各个品种基差的字典
    '''
    result_dict={}
    cursor = db.cursor()  # 创建数据库游标
    info_df=info_df.copy()
    input_info=input_info.copy()
    require_factors_name=['利润']                   #['利润','基差','库存']
    for i in input_info.keys():
        if i in require_factors_name:
            res_df=input_info[i][['f_code','data_name','db_table']].set_index('f_code')
            code_list=list(np.sort(list(set(info_df.index.tolist()).intersection(set(res_df.index.tolist())))))
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


#%% 定义计算基差因子函数
def cal_profit_factor_func(dict,special_item_list):
    # 构造工具
    tool_list=[]
    time_list=[datetime.strftime(i,'%Y%m%d') for i in pd.date_range(start='20100516',end=datetime.now())]
    tool_df=pd.DataFrame(['NAN']*len(time_list),index=time_list,columns=['tool'])
    tool_list.append(tool_df)
    # 计算因子值
    for code in dict.keys():
        if code in special_item_list:
            need_data_df=dict[code].fillna(method='ffill')
        else:
            need_data_df=dict[code]
        rank_df=need_data_df.rank(ascending=False)/len(need_data_df.index.tolist())
        rank_df=rank_df.loc[~rank_df.index.duplicated(keep='first')]
        tool_list.append(rank_df)
    factor_df=pd.concat(tool_list,axis=1)
    factor_data_df=factor_df.drop(columns=['tool']).dropna(axis=0,how='all').dropna(thresh=2).sort_index()

    open_ret_data = FACTORS.Get_daily_factor().daily_ret_all_open
    open_ret_data.index=[datetime.strftime(i,'%Y%m%d') for i in open_ret_data.index.tolist()]
    open_ret_data=open_ret_data.reindex(index=factor_data_df.index.tolist()).dropna(axis=0,how='all')

    factor_ret = pd.Series(index=open_ret_data.index.tolist())
    for date in factor_ret.index.tolist():
        factor_sort = factor_data_df.shift(1).loc[date].sort_values().dropna()
        symbol_num_20pct = round(len(factor_sort) * 0.2)
        factor_ret.loc[date] = open_ret_data.loc[date, factor_sort.iloc[-symbol_num_20pct:].index].mean() - \
                               open_ret_data.loc[date, factor_sort.iloc[:symbol_num_20pct].index].mean()
    factor_ret=factor_ret.fillna(method='ffill').fillna(method='bfill')
    return factor_ret

#%% 定义修改数据库数据函数（利润）
def modified_table_feature_profit_func():
    cursor = db.cursor()
    modified_dict={'zn':'元/金属吨','j':'元','SM':'元','AP':'元/斤','lh':'元/头','jd':'元/斤'}
    sql1="""UPDATE FACTOR_PAQHYJS_FEATURE  
SET F_UNIT = '元/金属吨'  
WHERE F_VARITY = 'zn' AND F_DATANAME='利润数值'
    """
    sql2 = """UPDATE FACTOR_PAQHYJS_FEATURE  
    SET F_UNIT = '元'  
    WHERE F_VARITY  = 'j' AND F_DATANAME='利润数值'
        """
    sql3 = """UPDATE FACTOR_PAQHYJS_FEATURE  
    SET F_UNIT = '元'  
    WHERE F_VARITY  = 'SM' AND F_DATANAME='利润数值'
        """
    sql4 = """UPDATE FACTOR_PAQHYJS_FEATURE  
    SET F_UNIT = '元/斤'  
    WHERE F_VARITY  = 'AP' AND F_DATANAME='利润数值'
        """
    sql5 = """UPDATE FACTOR_PAQHYJS_FEATURE  
        SET F_UNIT = '元/头'  
        WHERE F_VARITY  = 'lh' AND F_DATANAME='利润数值'
            """
    sql6 = """UPDATE FACTOR_PAQHYJS_FEATURE  
            SET F_UNIT = '元/斤'  
            WHERE F_VARITY  = 'jd' AND F_DATANAME='利润数值'
                """
    cursor.execute(sql1)
    cursor.execute(sql2)
    cursor.execute(sql3)
    cursor.execute(sql4)
    cursor.execute(sql5)
    cursor.execute(sql6)

#%% 定义利润因子入库或更新函数
def profit_factor_into_oracle_func():
    # %% 更新各品种利润特征
    print('开始插入或者更新各品种利润特征')
    date_of_today = time.strftime("%Y-%m-%d", time.localtime())
    code_info = pd.read_excel('C:\\Users\\Administrator\\PycharmProjects\\平安期货code\\平安货期代码\\业绩归因\\code_info.xlsx', index_col=0)   #########！
    res_info = pd.read_excel('C:\\Users\\Administrator\\PycharmProjects\\平安期货code\\平安货期代码\\数据库\\库存-利润-基差-汇总.xlsx', sheet_name=None)  ########！
    feature_dict = get_fundamental_profit_data_oracle(info_df=code_info, input_info=res_info)
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
        data_df['F_DATANAME'] = ['利润数值'] * len(feature_dict[code].sort_index(ascending=False).index.tolist())
        data_df['F_VALUE'] = feature_dict[code].sort_index(ascending=False)[code].values.tolist()
        data_df['F_UNIT'] = ['元/吨'] * len(feature_dict[code].sort_index(ascending=False).index.tolist())
        data_df['F_DATAREMARK'] = [date_of_today] * len(feature_dict[code].sort_index(ascending=False).index.tolist())
        update_feature_df(data_df)
    print('各品种利润特征插入或者更新结束')

    special_items_list = ['SA', 'jm', 'j', 'zc', 'AP', 'lh', 'CJ', 'jd']

    # %% 计算利润因子
    print('开始计算利润因子')
    basis_factor_ret = cal_profit_factor_func(dict=feature_dict, special_item_list=special_items_list)
    basis_factor_ret = basis_factor_ret.sort_index(ascending=False)
    print('结束计算利润因子')
    # %% 更新利润因子数值
    print('开始插入或者更新利润因子')
    basis_factor_data_df = pd.DataFrame(columns=['F_DATE', 'F_DATANAME', 'F_TABLENAME', 'F_VALUE', 'F_DATAREMARK'])
    basis_factor_data_df['F_DATE'] = basis_factor_ret.index.tolist()
    basis_factor_data_df['F_DATANAME'] = ['利润因子'] * len(basis_factor_ret.index.tolist())
    basis_factor_data_df['F_TABLENAME'] = ['基本面因子'] * len(basis_factor_ret.index.tolist())
    basis_factor_data_df['F_VALUE'] = basis_factor_ret.values.tolist()
    basis_factor_data_df['F_DATAREMARK'] = [date_of_today] * len(basis_factor_ret.index.tolist())
    update_factors_df(basis_factor_data_df)
    modified_table_feature_profit_func()
    print('插入或者更新利润因子结束')




#%%调用主函数
if __name__=='__main__':
    print('日频基本面特征和因子程序开始运行！')
    basis_factor_into_oracle_func()
    time.sleep(2)
    profit_factor_into_oracle_func()
    print('日频基本面特征和因子程序结束运行！！')


















