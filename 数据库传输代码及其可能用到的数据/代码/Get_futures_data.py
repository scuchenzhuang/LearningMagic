# -*- coding: utf-8 -*-
# @Time    : 2022/7/13 16:14
# @Author  : Chris
import datetime

import pandas as pd
import numpy as np
import time
import os
import pymongo        #处理mongoDB数据库
import configparser   #处理配置文件

CONFIGFILE_PATH = 'C:\\Users\\Administrator\\PycharmProjects\\平安期货code\\平安货期代码\\量价因子\\config.ini'   #配置ini文件

class Get_future_data(object):

    def __init__(self):
        config = configparser.ConfigParser()
        config.read(CONFIGFILE_PATH)
        host = config.get("db", "host")
        port = int(config.get('db', 'port'))
        self.db = pymongo.MongoClient(host, port).quote   # 连接数据库
        cont_info = self.db['t_futinstrument']    #？
        cursor = cont_info.find({})
        res = pd.DataFrame([item for item in cursor])
        self.expire_date_se = res[['symbol', 'expiredate']].set_index('symbol')
        self.expire_date_se = self.expire_date_se.groupby('symbol').max() # TA210 对应2022.10和2012.10
        self.code_info = res.groupby('code').first()[['name', 'exchange']].drop(['sc_tas', 'ER', 'LR', 'ME', 'RO', 'TC', 'WS', 'WT', 'bb'])
        self.code_info['name'] = self.code_info['name'].apply(lambda x:x.replace(' ', ''))    #？
        self.code_info['exchange'] = self.code_info['exchange'].apply(lambda x:x.replace(' ', ''))   #？
        res_sample = self.get_contract_his('a')  #？
        trade_date_list = res_sample['trading_day'].drop_duplicates().sort_values().to_list()
        self.trade_date_list = [pd.Timestamp(date) for date in trade_date_list if date > '20100516']   # 交易时间列表

    # 获取品种某个交易日的所有合约行情信息
    def get_contract(self, date, symbol):
        date = str(date)[:10].replace('-', '')   #？输入的时间格式2022-01-11
        s_price_col = self.db['t_settlementprice']    #？

        cursor = s_price_col.find({'trading_day': date, 'symbol': {'$regex': '^' + symbol}})
        res = pd.DataFrame([item for item in cursor])
        if len(res) == 0:
            cursor = s_price_col.find({'trading_day': date, 'symbol': {'$regex': '^' + symbol, '$options': 'i'}})
            res = pd.DataFrame([item for item in cursor])
            len_symbol = 3 + len(symbol)
        else:
            len_symbol = 4 + len(symbol)
        res = res[res['symbol'].apply(lambda x : len(x)) == len_symbol]
        return res

    # 获取品种某个交易日的价差
    def get_contract_diff_ratio(self, date, symbol):
        date = str(date)[:10].replace('-', '')
        res = self.get_contract(date, symbol)   #获取品种某个交易日的所有合约行情信息
        #res['expiredate'] = self.expire_date_se.loc[res['symbol']]['expiredate'].to_list()
        res['expiredate'] = self.expire_date_se.reindex(res['symbol'])['expiredate'].to_list()
        all_contract = res.set_index('symbol').sort_values('expiredate')
        arg_max = all_contract['volume'].argmax()
        close_prc_1 = all_contract['close_price'].iloc[arg_max]
        slice_contract = all_contract.iloc[arg_max+1:]
        arg_max_2 = slice_contract['volume'].argmax()
        close_prc_2 = slice_contract['close_price'].iloc[arg_max_2]
        if (close_prc_1==0) or (close_prc_2==0):
            return np.nan
        else:
            diff_ratio = (close_prc_1 - close_prc_2) / close_prc_1 / (arg_max_2+1) * 12   #年化价差的计算方式
            return diff_ratio

    # 获取品种从某个交易日开始的价差序列
    def get_contract_diff_ratio_his(self, symbol, start_date = ''):
        res = self.get_contract_his(symbol)
        res = res[res['trading_day']>'20100516']
        if start_date != '':
            res = res[res['trading_day'] >= str(start_date)[:10].replace('-', '')]

        def daily_diff(res_daily):
            try:
                res_daily['expiredate'] = self.expire_date_se['expiredate'].loc[res_daily['symbol']].tolist()
            except:
                print('缺最新合约信息数据')
                # res_daily = res_daily.iloc[:-1]
                res_daily = res_daily[res_daily['symbol'].apply(lambda x: x in self.expire_date_se['expiredate'].index)]
                res_daily['expiredate'] = self.expire_date_se['expiredate'].loc[res_daily['symbol']].tolist()
            all_contract = res_daily.set_index('symbol').sort_values('expiredate')
            try:
                arg_max = all_contract['volume'].argmax()
                close_prc_1 = all_contract['close_price'].iloc[arg_max]
                slice_contract = all_contract.iloc[arg_max + 1:]
                arg_max_2 = slice_contract['volume'].argmax()
                close_prc_2 = slice_contract['close_price'].iloc[arg_max_2]
            except:
                print(all_contract['trading_day'])
                return np.nan
            if (close_prc_1 == 0) or (close_prc_2 == 0):
                return np.nan
            else:
                diff_ratio = (close_prc_1 - close_prc_2) / close_prc_1 / (arg_max_2 + 1) * 12
                return diff_ratio

        diff_data = res.groupby('trading_day').apply(daily_diff)
        date_list = diff_data.reset_index()['trading_day'].apply(lambda x : pd.Timestamp(x)).tolist()
        diff = pd.Series(np.array(diff_data), index = date_list).sort_index()
        diff = diff[~diff.index.duplicated(keep='first')]
        return diff
    #获取所有品种的从某个交易日开始的价差序列
    def get_main_contract_diff_all(self, start_date = ''):
        diff_all = pd.DataFrame(columns = self.code_info.index)
        for code in self.code_info.index:
            diff_all[code] = self.get_contract_diff_ratio_his(code, start_date)
        return diff_all
    # def get_contract_his(self, symbol):
    #     s_price_col = self.db['t_settlementprice']
    #
    #     cursor = s_price_col.find({'symbol': {'$regex': '^' + symbol}})
    #     res = pd.DataFrame([item for item in cursor])
    #     if len(res) == 0:
    #         cursor = s_price_col.find({'symbol': {'$regex': '^' + symbol, '$options': 'i'}})
    #         res = pd.DataFrame([item for item in cursor])
    #         len_symbol = 3 + len(symbol)
    #     else:
    #         len_symbol = 4 + len(symbol)
    #     res = res[res['symbol'].apply(lambda x : len(x)) == len_symbol]
    #     return res

    # 获取某个品种全历史行情信息
    def get_contract_his(self, symbol):
        s_price_col = self.db['t_settlementprice']
        cursor = s_price_col.find({'symbol': {'$regex': '^' + symbol}})
        res = pd.DataFrame([item for item in cursor])
        if self.code_info.loc[symbol, 'exchange'] == 'CZCE':
            len_symbol = 3 + len(symbol)
        else:
            len_symbol = 4 + len(symbol)
        res['symbol'] = res['symbol'].apply(lambda x: x.replace(' ', ''))
        res['exchange'] = res['exchange'].apply(lambda x: x.replace(' ', ''))
        contract2code = lambda s: ''.join(x for x in s if x.isalpha())
        res = res[res['symbol'].apply(contract2code).apply(lambda x:x.lower()) == symbol.lower()]
        return res

    # 获取某个品种从某个交易日开始持仓量序列
    def get_open_interest(self, symbol, start_date = ''):
        res = self.get_contract_his(symbol)
        if start_date != '':
            res = res[res['trading_day'] >= str(start_date)[:10].replace('-', '')]
        open_interest_data = res.groupby('trading_day').apply(lambda x: x['open_interest'].sum())    #计算公式
        if type(open_interest_data) == pd.core.frame.DataFrame:
            open_interest_data = open_interest_data[open_interest_data.columns[0]]
        date_list = open_interest_data.reset_index()['trading_day'].apply(lambda x : pd.Timestamp(x)).tolist()
        open_interest = pd.Series(np.array(open_interest_data), index = date_list).sort_index()
        open_interest = open_interest[~open_interest.index.duplicated(keep='first')]
        return open_interest

    # 近月持仓量
    def get_open_interest_nearby(self, symbol, start_date = '', pos = 1):
        res = self.get_contract_his(symbol)
        if start_date != '':
            res = res[res['trading_day'] >= str(start_date)[:10].replace('-', '')]

        #res['expiredate'] = self.expire_date_se.loc[res['symbol']]['expiredate'].tolist()
        res['expiredate'] = self.expire_date_se.reindex(res['symbol'])['expiredate'].tolist()
        def daily_open_interest(res_daily):    #计算公式
            try:
                res_daily = res_daily.sort_values('expiredate')
                contract_data = res_daily.iloc[pos-1]
                open_interest = contract_data['open_interest']
                return open_interest
            except:
                return 0
        open_interest_data = res.groupby('trading_day').apply(daily_open_interest)
        if type(open_interest_data) == pd.core.frame.DataFrame:
            open_interest_data = open_interest_data[open_interest_data.columns[0]]
        date_list = open_interest_data.reset_index()['trading_day'].apply(lambda x : pd.Timestamp(x)).tolist()
        open_interest = pd.Series(np.array(open_interest_data), index = date_list).sort_index()
        open_interest = open_interest[~open_interest.index.duplicated(keep='first')]
        return open_interest
    # 获取某品种从某交易日开始的主力合约收益率序列（结算价）
    def get_main_contract_ret(self, symbol, start_date = ''):
        res = self.get_contract_his(symbol)
        if start_date != '':
            res = res[res['trading_day'] >= str(start_date)[:10].replace('-', '')]

        ret_data = res.groupby('trading_day').apply(lambda x: (x[x['volume'] == x['volume'].max()]['settle_price'] -
                                                               x[x['volume'] == x['volume'].max()]['pre_settle']) /
                                                              x[x['volume'] == x['volume'].max()]['pre_settle']
                                                                if x['volume'].max()>0 else pd.Series(0))
        if type(ret_data) == pd.core.frame.DataFrame:
            ret_data = ret_data[ret_data.columns[0]]
        # ret_data = res.groupby('trading_day').apply(daily_ret)
        date_list = ret_data.reset_index()['trading_day'].apply(lambda x : pd.Timestamp(x)).tolist()
        ret = pd.Series(np.array(ret_data), index = date_list).sort_index()
        ret = ret[~ret.index.duplicated(keep='first')]
        return ret

    # 获取某品种从某交易日开始的次主力合约收益率序列（结算价）
    def get_sec_main_contract_ret(self, symbol, start_date = ''):
        res = self.get_contract_his(symbol)
        res = res[res['trading_day']>'20100516']
        if start_date != '':
            res = res[res['trading_day'] >= str(start_date)[:10].replace('-', '')]
        def daily_ret(res_daily):
            try:
                res_daily['expiredate'] = self.expire_date_se['expiredate'].loc[res_daily['symbol']].tolist()
            except:
                # res_daily = res_daily.iloc[:-1]
                res_daily = res_daily[res_daily['symbol'].apply(lambda x: x in self.expire_date_se['expiredate'].index)]
                res_daily['expiredate'] = self.expire_date_se['expiredate'].loc[res_daily['symbol']].tolist()
                print('缺最新合约信息数据')
            all_contract = res_daily.set_index('symbol').sort_values('expiredate')
            try:
                arg_max = all_contract['volume'].argmax()
                slice_contract = all_contract.iloc[arg_max + 1:]
                contract_data = slice_contract[slice_contract['volume'] == slice_contract['volume'].max()]
                ret = (contract_data['settle_price'] - contract_data['pre_settle']) / contract_data['pre_settle']
                return ret
            except:
                print(all_contract['trading_day'])
                return 0

        ret_data = res.groupby('trading_day').apply(daily_ret)
        if type(ret_data) == pd.core.frame.DataFrame:
            ret_data = ret_data[ret_data.columns[0]]
        # ret_data = res.groupby('trading_day').apply(daily_ret)
        date_list = ret_data.reset_index()['trading_day'].apply(lambda x : pd.Timestamp(x)).tolist()
        ret = pd.Series(np.array(ret_data), index = date_list).sort_index()
        ret = ret[~ret.index.duplicated(keep='first')]
        return ret

    # 临近合约收益率
    def get_contract_ret_nearby(self, symbol, pos = 1):
        res = self.get_contract_his(symbol)
        #res['expiredate'] = self.expire_date_se.loc[res['symbol']]['expiredate'].tolist()
        res['expiredate'] = self.expire_date_se.reindex(res['symbol'])['expiredate'].tolist()
        def daily_ret(res_daily):
            try:
                res_daily = res_daily.sort_values('expiredate')
                if res_daily['turnover'].iloc[pos-1] > 0:
                    contract_data = res_daily.iloc[pos-1]
                    ret = (contract_data['settle_price'] - contract_data['pre_settle']) / contract_data['pre_settle']
                else:
                    contract_data = res_daily[res_daily['volume'] == res_daily['volume'].max()]
                    ret = ((contract_data['settle_price'] - contract_data['pre_settle']) / contract_data['pre_settle']).item()
                return ret
            except:
                return 0
        ret_data = res.groupby('trading_day').apply(daily_ret)
        if type(ret_data) == pd.core.frame.DataFrame:
            ret_data = ret_data[ret_data.columns[0]]
        # ret_data = res.groupby('trading_day').apply(daily_ret)
        date_list = ret_data.reset_index()['trading_day'].apply(lambda x : pd.Timestamp(x)).tolist()
        ret = pd.Series(np.array(ret_data), index = date_list).sort_index()
        ret = ret[~ret.index.duplicated(keep='first')]
        return ret

    # 开盘价计价的日收益率：(次日开盘-当日开盘)/(当日开盘) 最后一日用 (当日收盘-当日开盘)/(当日开盘)
    def get_main_contract_ret_open(self, symbol, start_date = ''):
        res = self.get_contract_his(symbol)
        date_list = res['trading_day'].sort_values().drop_duplicates().tolist()
        open_ret = pd.Series(index=date_list)
        range_start = 0
        if start_date != '':
            start_date = str(start_date)[:10].replace('-', '')
            open_ret = open_ret.loc[start_date:]
            try:
                range_start = date_list.index(start_date)
            except:
                #return open_ret
                #range_start = 0
                pass
        for i in range(range_start, len(date_list)):
            date = date_list[i]
            daily_res = res[res['trading_day'] == date]
            if daily_res['volume'].max() == 0:
                open_ret.loc[date] = 0
                continue
            open_price = daily_res.iloc[daily_res['volume'].argmax()]['open_price'].item()
            contract = daily_res.iloc[daily_res['volume'].argmax()]['symbol']
            if i < len(date_list) - 1:
                date_next = date_list[i + 1]
                daily_res_next = res[res['trading_day'] == date_next]
                if daily_res_next['volume'].max() == 0:
                    open_ret.loc[date] = 0
                    continue
                if contract not in daily_res_next['symbol'].to_list():
                    open_ret.loc[date] = 0
                    continue
                try:
                    open_price_next = daily_res_next[daily_res_next['symbol'] == contract]['open_price'].item()
                except:
                    open_ret.loc[date] = 0
                    continue
                if (open_price_next == 0) or (open_price == 0):
                    open_ret.loc[date] = 0
                    continue
                open_ret.loc[date] = (open_price_next - open_price) / open_price
            else:
                try:
                    close_price = daily_res.iloc[daily_res['volume'].argmax()]['close_price'].item() if daily_res['volume'].max() > 0 else 0
                except:
                    open_ret.loc[date] = 0
                    continue
                if (close_price == 0) or (open_price == 0):
                    open_ret.loc[date] = 0
                    continue
                open_ret.loc[date] = (close_price - open_price) / open_price
            open_ret = pd.Series(np.array(open_ret), index=[pd.Timestamp(x) for x in open_ret.index.to_list()])
        return open_ret

    # 收盘计价的日收益率：(当日收盘-前日收盘)/(前日收盘) 第一日用 (当日收盘-当日开盘)/(当日开盘)
    def get_main_contract_ret_close(self, symbol, start_date=''):
        res = self.get_contract_his(symbol)
        date_list = res['trading_day'].sort_values().drop_duplicates().tolist()
        close_ret = pd.Series(index=date_list)
        range_start = 0
        if start_date != '':
            start_date = str(start_date)[:10].replace('-', '')
            close_ret = close_ret.loc[start_date:]
            try:
                range_start = date_list.index(start_date)
            except:
                # return close_ret
                #range_start = 0
                pass
        for i in range(range_start, len(date_list)):
            date = date_list[i]
            daily_res = res[res['trading_day'] == date]
            if daily_res['volume'].max() == 0:
                close_ret.loc[date] = 0
                continue
            close_price = daily_res.iloc[daily_res['volume'].argmax()]['close_price'].item()
            contract = daily_res.iloc[daily_res['volume'].argmax()]['symbol']
            if i >= 1:
                date_pre = date_list[i - 1]
                daily_res_pre = res[res['trading_day'] == date_pre]
                if daily_res_pre['volume'].max() == 0:
                    close_ret.loc[date] = 0
                    continue
                if contract not in daily_res_pre['symbol'].to_list():
                    close_ret.loc[date] = 0
                    continue
                try:
                    close_price_pre = daily_res_pre[daily_res_pre['symbol'] == contract]['close_price'].item()
                except:
                    close_ret.loc[date] = 0
                    continue
                if (close_price == 0) or (close_price_pre == 0):
                    close_ret.loc[date] = 0
                    continue
                close_ret.loc[date] = (close_price - close_price_pre) / close_price_pre
            else:
                try:
                    open_price = daily_res.iloc[daily_res['volume'].argmax()]['open_price'].item() if daily_res['volume'].max() > 0 else 0
                except:
                    close_ret.loc[date] = 0
                    continue
                if (close_price == 0) or (open_price == 0):
                    close_ret.loc[date] = 0
                    continue
                close_ret.loc[date] = (close_price - open_price) / open_price
            close_ret = pd.Series(np.array(close_ret), index=[pd.Timestamp(x) for x in close_ret.index.to_list()])
        return close_ret

    # 获取所有品种从某交易日开始的主力合约收益率序列（结算价、收盘价、开盘价）
    def get_main_contract_ret_all(self, type = 'settle', start_date = ''):

        date_list = pd.date_range(start=start_date, end=str(datetime.datetime.now()))
        date_list = [pd.Timestamp(x) for x in date_list]

        ret_all = pd.DataFrame(index=date_list,columns = self.code_info.index)
        for code in self.code_info.index:
            if type == 'settle':
                ret_all[code] = self.get_main_contract_ret(code, start_date)
            elif type == 'close':
                ret_all[code] = self.get_main_contract_ret_close(code, start_date)
            elif type == 'open':
                ret_all[code] = self.get_main_contract_ret_open(code, start_date)
            else:
                print('incorrect type!')

        ret_all.dropna(axis=0, how='all', inplace=True)
        #ret_all.drop(index=ret_all.index[-1], inplace=True)

        return ret_all

    # 获取所有品种从某交易日开始的次主力合约收益率序列（结算价）
    def get_sec_main_contract_ret_all(self, start_date = ''):
        ret_all = pd.DataFrame(columns = self.code_info.index)
        for code in self.code_info.index:
            ret_all[code] = self.get_sec_main_contract_ret(code, start_date)
        return ret_all

    # 获取所有品种从某交易日开始的持仓量序列
    def get_open_interest_all(self, start_date = ''):
        open_interest_all = pd.DataFrame(columns = self.code_info.index)
        for code in self.code_info.index:
            open_interest_all[code] = self.get_open_interest(code, start_date)
        return open_interest_all

    def get_symbol_daily(self, date, symbol):
        pass

# class Get_future_data(object):
#
#     def __init__(self):
#         config = configparser.ConfigParser()
#         config.read(CONFIGFILE_PATH)
#         host = config.get("db", "host")
#         port = int(config.get('db', 'port'))
#         self.db = pymongo.MongoClient(host, port).quote
#
#     # 单品种单日
#     def get_contract(self, date, symbol):
#         date = str(date).replace('-', '')
#         s_price_col = self.db['t_settlementprice']
#         cursor = s_price_col.find({'symbol': {'$regex': symbol}, 'trading_day': date}).limit(5)
#         res = pd.DataFrame([item for item in cursor])
#         return res
#
#     # 单品种全历史
#     def get_contract_his(self, symbol):
#         s_price_col = self.db['t_settlementprice']
#         cursor = s_price_col.find({'symbol': {'$regex': symbol}}).limit(5)
#         res = pd.DataFrame([item for item in cursor])
#         return res
#
#     def get_symbol_daily(self, date, symbol):
#         pass
#
#     def get_all_symbol(self, start_date, end_date):
#         pass
#
#     def get_all_symbol_cont(self, start_date, end_date):
#         pass

    ###################################################################################
    # 收盘计价的次主力合约的日收益率：(当日收盘-前日收盘)/(前日收盘)  第一日用 (当日收盘-当日开盘)/(当日开盘)
    def get_sec_main_contract_ret_close(self, symbol, start_date=''):
        pd.set_option('mode.chained_assignment', None)   # 不提示不必要的警告
        res = self.get_contract_his(symbol)
        date_list = res['trading_day'].sort_values().drop_duplicates().tolist()
        close_ret = pd.Series(index=date_list)
        range_start = 0
        if start_date != '':
            start_date = str(start_date)[:10].replace('-', '')
            close_ret = close_ret.loc[start_date:]
            try:
                range_start = date_list.index(start_date)
            except:
                pass
        for i in range(range_start, len(date_list)):
            date = date_list[i]
            daily_res = res[res['trading_day'] == date]
            if daily_res['volume'].max() == 0:
                close_ret.loc[date] = 0
                continue
            try:
                daily_res['expiredate'] = self.expire_date_se['expiredate'].loc[daily_res['symbol']].tolist()
            except:
                daily_res = daily_res[daily_res['symbol'].apply(lambda x: x in self.expire_date_se['expiredate'].index)]
                daily_res['expiredate'] = self.expire_date_se['expiredate'].loc[daily_res['symbol']].tolist()
                print('缺最新合约信息数据')
            daily_res=daily_res.set_index('symbol').sort_values('expiredate').reset_index('symbol')
            argmax_id = daily_res['volume'].argmax()
            daily_res=daily_res.iloc[argmax_id+1:,:]
            if not daily_res.empty:
                close_price = daily_res.iloc[daily_res['volume'].argmax()]['close_price'].item()
                contract = daily_res.iloc[daily_res['volume'].argmax()]['symbol']
                if i >= 1:
                    date_pre = date_list[i - 1]
                    daily_res_pre = res[res['trading_day'] == date_pre]
                    if daily_res_pre['volume'].max() == 0:
                        close_ret.loc[date] = 0
                        continue
                    if contract not in daily_res_pre['symbol'].to_list():
                        close_ret.loc[date] = 0
                        continue
                    try:
                        close_price_pre = daily_res_pre[daily_res_pre['symbol'] == contract]['close_price'].item()
                    except:
                        close_ret.loc[date] = 0
                        continue
                    if (close_price == 0) or (close_price_pre == 0):
                        close_ret.loc[date] = 0
                        continue
                    close_ret.loc[date] = (close_price - close_price_pre) / close_price_pre
                else:
                    try:
                        open_price = daily_res.iloc[daily_res['volume'].argmax()]['open_price'].item() if daily_res['volume'].max() > 0 else 0
                    except:
                        close_ret.loc[date] = 0
                        continue
                    if (close_price == 0) or (open_price == 0):
                        close_ret.loc[date] = 0
                        continue
                    close_ret.loc[date] = (close_price - open_price) / open_price
                close_ret = pd.Series(np.array(close_ret), index=[pd.Timestamp(x) for x in close_ret.index.to_list()])
            else:
                continue
        return close_ret

    # 获取所有品种从某交易日开始的次主力合约收益率序列（按收盘价计算）
    def get_sec_main_contract_ret_all_close(self, start_date=''):

        date_list=pd.date_range(start=start_date,end=str(datetime.datetime.now()))
        date_list=[pd.Timestamp(x) for x in date_list]

        ret_all = pd.DataFrame(index=date_list,columns=self.code_info.index)
        for code in self.code_info.index:
            ret_all[code] = self.get_sec_main_contract_ret_close(code, start_date)

        ret_all.dropna(axis=0,how='all',inplace=True)
        #ret_all.drop(index=ret_all.index[-1],inplace=True)

        return ret_all

    # 获取主力合约的成交量序列
    def get_main_contract_volume(self,symbol,start_date=''):
        res = self.get_contract_his(symbol)
        date_list = res['trading_day'].sort_values().drop_duplicates().tolist()
        volume_series = pd.Series(index=date_list)
        range_start = 0
        if start_date != '':
            start_date = str(start_date)[:10].replace('-', '')
            volume_series = volume_series.loc[start_date:]
            try:
                range_start = date_list.index(start_date)
            except:
                pass
        for i in range(range_start, len(date_list)):
            date = date_list[i]
            daily_res = res[res['trading_day'] == date]
            if daily_res['volume'].max() == 0:
                volume_series.loc[date] = 0
                continue
            volume = daily_res.iloc[daily_res['volume'].argmax()]['volume'].item()
            volume_series.loc[date]=volume
        return volume_series


    # 获取所有品种的主力合约成交量序列
    def get_main_contract_volume_all(self,start_date=''):
        ret_all=pd.DataFrame(columns=self.code_info.index)
        for code in self.code_info.index:
            ret_all[code]=self.get_main_contract_volume(code, start_date)
        return ret_all


    # 获取主力合约的成交金额序列
    def get_main_contract_turnover(self, symbol, start_date=''):
        res = self.get_contract_his(symbol)
        date_list = res['trading_day'].sort_values().drop_duplicates().tolist()
        turnover_series = pd.Series(index=date_list)
        range_start = 0
        if start_date != '':
            start_date = str(start_date)[:10].replace('-', '')
            turnover_series = turnover_series.loc[start_date:]
            try:
                range_start = date_list.index(start_date)
            except:
                pass
        for i in range(range_start, len(date_list)):
            date = date_list[i]
            daily_res = res[res['trading_day'] == date]
            if daily_res['volume'].max() == 0:
                turnover_series.loc[date] = 0
                continue
            turnover = daily_res.iloc[daily_res['volume'].argmax()]['turnover'].item()
            turnover_series.loc[date] = turnover
        return turnover_series


    # 获取所有品种的主力合约成交金额序列
    def get_main_contract_turnover_all(self, start_date=''):
        ret_all=pd.DataFrame(columns=self.code_info.index)
        for code in self.code_info.index:
            ret_all[code]=self.get_main_contract_turnover(code, start_date)
        return ret_all


    # 获取主力合约的收盘价序列
    def get_main_contract_close(self, symbol, start_date=''):
        res = self.get_contract_his(symbol)
        date_list = res['trading_day'].sort_values().drop_duplicates().tolist()
        close_series = pd.Series(index=date_list)
        range_start = 0
        if start_date != '':
            start_date = str(start_date)[:10].replace('-', '')
            close_series = close_series.loc[start_date:]
            try:
                range_start = date_list.index(start_date)
            except:
                pass
        for i in range(range_start, len(date_list)):
            date = date_list[i]
            daily_res = res[res['trading_day'] == date]
            if daily_res['volume'].max() == 0:
                close_series.loc[date] = np.nan
                continue
            try:
                close = daily_res.iloc[daily_res['volume'].argmax()]['close_price'].item()
            except:
                close_series.loc[date]=np.nan
                continue
            close_series.loc[date] = close
        return close_series

    # 获取所有品种的主力合约收盘价序列
    def get_main_contract_close_all(self, start_date=''):
        ret_all=pd.DataFrame(columns=self.code_info.index)
        for code in self.code_info.index:
            ret_all[code]=self.get_main_contract_close(code, start_date)
        return ret_all

    # 获取主力合约的开盘价序列
    def get_main_contract_open(self, symbol, start_date=''):
        res = self.get_contract_his(symbol)
        date_list = res['trading_day'].sort_values().drop_duplicates().tolist()
        close_series = pd.Series(index=date_list)
        range_start = 0
        if start_date != '':
            start_date = str(start_date)[:10].replace('-', '')
            close_series = close_series.loc[start_date:]
            try:
                range_start = date_list.index(start_date)
            except:
                pass
        for i in range(range_start, len(date_list)):
            date = date_list[i]
            daily_res = res[res['trading_day'] == date]
            if daily_res['volume'].max() == 0:
                close_series.loc[date] = np.nan
                continue
            try:
                close = daily_res.iloc[daily_res['volume'].argmax()]['open_price'].item()
            except:
                close_series.loc[date]=np.nan
                continue
            close_series.loc[date] = close
        return close_series

    # 获取所有品种的主力合约开盘价序列
    def get_main_contract_open_all(self, start_date=''):
        ret_all=pd.DataFrame(columns=self.code_info.index)
        for code in self.code_info.index:
            ret_all[code]=self.get_main_contract_open(code, start_date)
        return ret_all

    # 获取主力合约的结算价序列
    def get_main_contract_settle(self, symbol, start_date=''):
        res = self.get_contract_his(symbol)
        date_list = res['trading_day'].sort_values().drop_duplicates().tolist()
        close_series = pd.Series(index=date_list)
        range_start = 0
        if start_date != '':
            start_date = str(start_date)[:10].replace('-', '')
            close_series = close_series.loc[start_date:]
            try:
                range_start = date_list.index(start_date)
            except:
                pass
        for i in range(range_start, len(date_list)):
            date = date_list[i]
            daily_res = res[res['trading_day'] == date]
            if daily_res['volume'].max() == 0:
                close_series.loc[date] = np.nan
                continue
            try:
                close = daily_res.iloc[daily_res['volume'].argmax()]['settle_price'].item()
            except:
                close_series.loc[date]=np.nan
                continue
            close_series.loc[date] = close
        return close_series

    # 获取所有品种主力合约结算价序列
    def get_main_contract_settle_all(self, start_date=''):
        ret_all=pd.DataFrame(columns=self.code_info.index)
        for code in self.code_info.index:
            ret_all[code]=self.get_main_contract_settle(code, start_date)
        return ret_all

    # 获取主力合约的ID
    def get_main_contract_id(self, symbol, start_date=''):
        res = self.get_contract_his(symbol)
        date_list = res['trading_day'].sort_values().drop_duplicates().tolist()
        close_series = pd.Series(index=date_list)
        range_start = 0
        if start_date != '':
            start_date = str(start_date)[:10].replace('-', '')
            close_series = close_series.loc[start_date:]
            try:
                range_start = date_list.index(start_date)
            except:
                pass
        for i in range(range_start, len(date_list)):
            date = date_list[i]
            daily_res = res[res['trading_day'] == date]
            if daily_res['volume'].max() == 0:
                close_series.loc[date] = np.nan
                continue
            close = daily_res.iloc[daily_res['volume'].argmax()]['symbol']
            close_series.loc[date] = close
        return close_series

    # 获取所有品种主力合约ID
    def get_main_contract_id_all(self, start_date=''):
        ret_all = pd.DataFrame(columns=self.code_info.index)
        for code in self.code_info.index:
            ret_all[code] = self.get_main_contract_id(code, start_date)
        return ret_all

    # 获取主力合约的最高价序列
    def get_main_contract_high(self, symbol, start_date=''):
        res = self.get_contract_his(symbol)
        date_list = res['trading_day'].sort_values().drop_duplicates().tolist()
        close_series = pd.Series(index=date_list)
        range_start = 0
        if start_date != '':
            start_date = str(start_date)[:10].replace('-', '')
            close_series = close_series.loc[start_date:]
            try:
                range_start = date_list.index(start_date)
            except:
                pass
        for i in range(range_start, len(date_list)):
            date = date_list[i]
            daily_res = res[res['trading_day'] == date]
            if daily_res['volume'].max() == 0:
                close_series.loc[date] = np.nan
                continue
            try:
                close = daily_res.iloc[daily_res['volume'].argmax()]['high_price'].item()
            except:
                close_series.loc[date]=np.nan
                continue
            close_series.loc[date] = close
        return close_series

    # 获取所有品种主力合约最高价序列
    def get_main_contract_high_all(self, start_date=''):
        ret_all = pd.DataFrame(columns=self.code_info.index)
        for code in self.code_info.index:
            ret_all[code] = self.get_main_contract_high(code, start_date)
        return ret_all

    # 获取主力合约的最低价序列
    def get_main_contract_low(self, symbol, start_date=''):
        res = self.get_contract_his(symbol)
        date_list = res['trading_day'].sort_values().drop_duplicates().tolist()
        close_series = pd.Series(index=date_list)
        range_start = 0
        if start_date != '':
            start_date = str(start_date)[:10].replace('-', '')
            close_series = close_series.loc[start_date:]
            try:
                range_start = date_list.index(start_date)
            except:
                pass
        for i in range(range_start, len(date_list)):
            date = date_list[i]
            daily_res = res[res['trading_day'] == date]
            if daily_res['volume'].max() == 0:
                close_series.loc[date] = np.nan
                continue
            try:
                close = daily_res.iloc[daily_res['volume'].argmax()]['low_price'].item()
            except:
                close_series.loc[date]=np.nan
                continue
            close_series.loc[date] = close
        return close_series

    # 获取所有品种主力合约最低价序列
    def get_main_contract_low_all(self, start_date=''):
        ret_all = pd.DataFrame(columns=self.code_info.index)
        for code in self.code_info.index:
            ret_all[code] = self.get_main_contract_low(code, start_date)
        return ret_all

    # 获取主力合约的昨结序列
    def get_main_contract_pre_settle(self, symbol, start_date=''):
        res = self.get_contract_his(symbol)
        date_list = res['trading_day'].sort_values().drop_duplicates().tolist()
        close_series = pd.Series(index=date_list)
        range_start = 0
        if start_date != '':
            start_date = str(start_date)[:10].replace('-', '')
            close_series = close_series.loc[start_date:]
            try:
                range_start = date_list.index(start_date)
            except:
                pass
        for i in range(range_start, len(date_list)):
            date = date_list[i]
            daily_res = res[res['trading_day'] == date]
            if daily_res['volume'].max() == 0:
                close_series.loc[date] = np.nan
                continue
            try:
                close = daily_res.iloc[daily_res['volume'].argmax()]['pre_settle'].item()
            except:
                close_series.loc[date]=np.nan
                continue
            close_series.loc[date] = close
        return close_series

    # 获取所有品种主力合约昨结序列
    def get_main_contract_pre_settle_all(self, start_date=''):
        ret_all = pd.DataFrame(columns=self.code_info.index)
        for code in self.code_info.index:
            ret_all[code] = self.get_main_contract_pre_settle(code, start_date)
        return ret_all

    # 获取某合约的所有开盘价、收盘价、最高价、最低价、结算价、昨结价
    def get_period_contract_price(self,contract_name,symbol):
        res = self.get_contract_his(symbol)
        con_res=res[res['symbol']==contract_name].set_index('trading_day').sort_index()[['open_price','close_price','high_price','low_price','settle_price','pre_settle']]
        return con_res







