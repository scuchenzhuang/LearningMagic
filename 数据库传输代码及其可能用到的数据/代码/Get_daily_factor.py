# -*- coding: utf-8 -*-
# @Time    : 2022/7/13 16:25
# @Author  : Chris

import pandas as pd
import numpy as np
from Get_futures_data import Get_future_data
from Basic_method import Basic_method
import matplotlib.pyplot as plt
import statsmodels.api as sm
import seaborn as sns
from matplotlib.font_manager import FontProperties   #与设置中文字体有关
from matplotlib.ticker import PercentFormatter
import os
from pulp import *

myfont=FontProperties(fname=r'C:\Users\Administrator\PycharmProjects\平安期货code\平安货期代码\中文字体管理文件\Fonts\simhei.ttf',size=14)    #设置字体管理器，导入字体的样式，可以指定为本地.ttf的文件(全局为ttf文件、局部为ttc文件)
sns.set(font=myfont.get_name(), style = 'white')   #设定图片样式
current_path = os.path.dirname(__file__)
os.chdir(current_path)

class Get_daily_factor(Get_future_data, Basic_method):

    def __init__(self):
        Get_future_data.__init__(self)
        Basic_method.__init__(self)
        abs_dir = 'C:\\Users\\Administrator\\PycharmProjects\\平安期货code\\平安货期代码\\量价因子'
        self.daily_ret_all = pd.read_parquet(abs_dir + '\\market_data\\daily_ret_all.parquet')
        self.daily_ret_all=self.daily_ret_all.loc[~self.daily_ret_all.index.duplicated(keep='first')]
        self.daily_sec_ret_all = pd.read_parquet(abs_dir + '\\market_data\\daily_sec_ret_all.parquet')
        self.daily_sec_ret_all=self.daily_sec_ret_all.loc[~self.daily_sec_ret_all.index.duplicated(keep='first')]
        self.diff_ratio_all = pd.read_parquet(abs_dir + '\\market_data\\diff_ratio_all.parquet')
        self.diff_ratio_all=self.diff_ratio_all.loc[~self.diff_ratio_all.index.duplicated(keep='first')]
        self.daily_ret_all_close = pd.read_parquet(abs_dir + '\\market_data\\close_ret_all.parquet')
        self.daily_ret_all_close=self.daily_ret_all_close.loc[~self.daily_ret_all_close.index.duplicated(keep='first')]
        self.daily_ret_all_open = pd.read_parquet(abs_dir + '\\market_data\\open_ret_all.parquet')
        self.daily_ret_all_open=self.daily_ret_all_open.loc[~self.daily_ret_all_open.index.duplicated(keep='first')]

        try:
            self.daily_main_contract_volume_all=pd.read_parquet(abs_dir+'\\market_data\\main_contract_volume_all.parquet')
            self.daily_main_contract_volume_all.index=[pd.Timestamp(x) for x in self.daily_main_contract_volume_all.index.tolist()]

            self.daily_main_contract_turnover_all=pd.read_parquet(abs_dir+'\\market_data\\main_contract_turnover_all.parquet')
            self.daily_main_contract_turnover_all.index=[pd.Timestamp(x) for x in self.daily_main_contract_turnover_all.index.tolist()]
        except:
            pass

        drop_exchange = ['CFFEX']
        code_info = pd.read_excel('C:\\Users\\Administrator\\PycharmProjects\\平安期货code\\平安货期代码\\业绩归因\\code_info.xlsx')
        self.cate_info = code_info[code_info['exchange'].apply(lambda x: x not in drop_exchange)]
        self.commodity_code_list = self.cate_info['code'].to_list()

        self.trade_code_list = self.cate_info['code'].to_list()
        delete_list = ['PM', 'JR', 'RI', 'RS', 'WH', 'wr', 'fb', 'CY', 'bc', 'rr']
        for code in delete_list:
            try:
                self.trade_code_list.remove(code)
            except:
                continue

        self.stock_index_futures_code_list=['IC','IF','IH']
        self.treasury_index_futures_code_list=['TS','TF','T']


    def update_data(self):    #更新本地数据集
        abs_dir = 'C:\\Users\\Administrator\\PycharmProjects\\平安期货code\\平安货期代码\\量价因子'
        res_sample = self.get_contract_his('rb')
        date_list = res_sample['trading_day'].drop_duplicates().sort_values().to_list()
        date_i = date_list.index(str(self.daily_ret_all.index[-1])[:10].replace('-',''))
        start_date = pd.Timestamp(date_list[date_i+1])
        start_date_last = pd.Timestamp(date_list[date_i])
        if start_date <= self.daily_ret_all.index[-1]:
            print('已更新至最新')
        else:
            print('正在更新')
            self.daily_ret_all = pd.concat([self.daily_ret_all, self.get_main_contract_ret_all(type = 'settle', start_date = start_date)]).drop_duplicates()
            self.daily_sec_ret_all = pd.concat([self.daily_sec_ret_all, self.get_sec_main_contract_ret_all_close(start_date = start_date)]).drop_duplicates()
            self.diff_ratio_all = pd.concat([self.diff_ratio_all, self.get_main_contract_diff_all(start_date = start_date)]).drop_duplicates()
            self.daily_ret_all_close = pd.concat([self.daily_ret_all_close, self.get_main_contract_ret_all(type = 'close', start_date = start_date)]).drop_duplicates()
            self.daily_ret_all_open = pd.concat([self.daily_ret_all_open.iloc[:-1], self.get_main_contract_ret_all(type = 'open', start_date = start_date_last)]).drop_duplicates()

            self.daily_ret_all.to_parquet(abs_dir + '\\market_data\\daily_ret_all.parquet')
            self.daily_sec_ret_all.to_parquet(abs_dir + '\\market_data\\daily_sec_ret_all.parquet')
            self.diff_ratio_all.to_parquet(abs_dir + '\\market_data\\diff_ratio_all.parquet')
            self.daily_ret_all_close.to_parquet(abs_dir + '\\market_data\\close_ret_all.parquet')
            self.daily_ret_all_open.to_parquet(abs_dir + '\\market_data\\open_ret_all.parquet')

            self.get_main_contract_volume_all(start_date='').sort_index().drop_duplicates().to_parquet(abs_dir + '\\market_data\\main_contract_volume_all.parquet')
            self.get_main_contract_turnover_all(start_date='').sort_index().drop_duplicates().to_parquet(abs_dir + '\\market_data\\main_contract_turnover_all.parquet')

            print('此轮更新结束')



    # 截面动量
    def momentum(self, roll_window = 5):
        factor = (self.daily_ret_all_close + 1).rolling(roll_window).apply(lambda x: x.prod())
        return factor

    # 偏度
    def skewness(self, roll_window = 255):
        factor = (self.daily_ret_all_close).rolling(roll_window).skew()
        return factor

    # 波动率
    def volatility(self, roll_window = 5):
        factor = self.daily_ret_all_close.rolling(roll_window).var() / self.daily_ret_all_close.abs().rolling(roll_window).mean()
        return factor

    # 展期收益率
    def diff_ratio(self):
        factor = self.diff_ratio_all
        return factor

    # 基差动量
    def basis_momentum(self, roll_window = 22):
        ret = (self.daily_ret_all_close + 1).rolling(roll_window).apply(lambda x: x.prod())
        sec_ret = (self.daily_sec_ret_all + 1).rolling(roll_window).apply(lambda x: x.prod())
        factor = ret - sec_ret
        return factor
    # 因子测试
    def factor_test(self, factor_df, type = 'sort', show = True,test_date=None):

        factor_df = factor_df.dropna(thresh=2).reindex(columns = self.trade_code_list)

        factor_ret = pd.Series(index=factor_df.index)

        for date in factor_df.index:
            if type == 'sort':   #计算因子的截面多空收益率
                factor_sort = factor_df.shift(1).loc[date].sort_values().dropna()
                symbol_num_20pct = round(len(factor_sort) * 0.2)
                factor_ret.loc[date] = self.daily_ret_all_open.loc[date, factor_sort.iloc[-symbol_num_20pct:].index].mean() - \
                                       self.daily_ret_all_open.loc[date, factor_sort.iloc[:symbol_num_20pct].index].mean()
            elif type == 'bool':  #计算因子的大于1与非大于1差额收益率
                factor_bool = (factor_df.shift(1).loc[date].dropna() > 1)
                factor_ret.loc[date] = self.daily_ret_all_open.loc[date, factor_bool[factor_bool].index].mean() - \
                                       self.daily_ret_all_open.loc[date, factor_bool[~factor_bool].index].mean()

        if test_date!=None:
            factor_ret=factor_ret.loc[pd.Timestamp(test_date):]

        net_value = np.cumprod(1 + factor_ret.dropna())
        if show:
            plt.clf()
            plt.plot(net_value)
            plt.show()
        return [factor_ret, net_value]    #第一个为因子回报率；第二个为累积净值
    # 周度更新因子
    def weekly_factor_report(self):
        factor_name_list = ['短期截面动量', '长期截面动量', '展期收益率', '基差动量', '偏度', '波动率']

        factor_ret_all = pd.DataFrame(columns = factor_name_list)
        factor_ret_all['短期截面动量'] = self.factor_test(self.momentum(), show=False)[0]
        factor_ret_all['长期截面动量'] = self.factor_test(self.momentum(20), show=False)[0]
        factor_ret_all['展期收益率'] = self.factor_test(self.diff_ratio(), show=False)[0]
        factor_ret_all['基差动量'] = self.factor_test(self.basis_momentum(), show=False)[0]
        factor_ret_all['偏度'] = self.factor_test(self.skewness(), show=False)[0]
        factor_ret_all['波动率'] = self.factor_test(self.volatility(), show=False)[0]
        return factor_ret_all
    # 计算所有因子值
    def cal_factor_all(self):
        self.factor_all_dict = {}
        self.factor_all_dict['短期截面动量'] = self.momentum()
        self.factor_all_dict['长期截面动量'] = self.momentum(20)
        self.factor_all_dict['展期收益率'] = self.diff_ratio()
        self.factor_all_dict['基差动量'] = self.basis_momentum()
        self.factor_all_dict['偏度'] = self.skewness()
        self.factor_all_dict['波动率'] = self.volatility()

        return self.factor_all_dict
    # 计算cta策略因子值(因子值规范化到-1到1之间)
    def cal_factor_strategy(self):
        def factor_neut(factor):
            return ((factor.rank() / len(factor.dropna())) - 0.5) * 2   #中性化
        self.factor_strategy_dict = {}
        self.vol_long_term = (self.daily_ret_all.rolling(120).std() + self.daily_ret_all.rolling(60).std() + self.daily_ret_all.rolling(20).std()).apply(factor_neut).reindex(columns=self.trade_code_list)
        self.factor_strategy_dict['短期截面动量'] = self.momentum().apply(factor_neut).reindex(columns = self.trade_code_list,index=self.vol_long_term.index.tolist())
        self.factor_strategy_dict['长期截面动量'] = self.momentum(20).apply(factor_neut).reindex(columns = self.trade_code_list,index=self.vol_long_term.index.tolist())
        self.factor_strategy_dict['展期收益率'] = self.diff_ratio().apply(factor_neut).reindex(columns = self.trade_code_list,index=self.vol_long_term.index.tolist())
        self.factor_strategy_dict['基差动量2d'] = self.basis_momentum(2).apply(factor_neut).reindex(columns = self.trade_code_list,index=self.vol_long_term.index.tolist())
        self.factor_strategy_dict['基差动量4d'] = self.basis_momentum(4).apply(factor_neut).reindex(columns = self.trade_code_list,index=self.vol_long_term.index.tolist())
        self.factor_strategy_dict['波动率3d'] = self.volatility(3).apply(factor_neut).reindex(columns = self.trade_code_list,index=self.vol_long_term.index.tolist())
        self.factor_strategy_dict['波动率6d'] = self.volatility(6).apply(factor_neut).reindex(columns = self.trade_code_list,index=self.vol_long_term.index.tolist())
        #self.factor_strategy_dict['长期波动率趋势']=self.vol_long_term

        return self.factor_strategy_dict
    # 根据因子值排序在一定的风险敞口下生成持仓权重
    def hold_weight(self, start_year = '2016', weight_list = [1,1,2,1,1,1,1]):   #？多加了长期波动率趋势因子值进入字典，所以权重多加了一项
        self.cal_factor_strategy()
        factor_sum = pd.DataFrame()
        factor_weight = pd.Series(weight_list, index = self.factor_strategy_dict.keys())
        for factorName in self.factor_strategy_dict.keys():
            if len(factor_sum) == 0:   #?
                factor_sum = self.factor_strategy_dict[factorName].loc[start_year:] * factor_weight.loc[factorName]
            else:
                factor_sum = factor_sum + self.factor_strategy_dict[factorName].loc[start_year:] * factor_weight.loc[factorName]
        factor_sum = factor_sum / factor_weight.sum()

        hold_weight = pd.DataFrame(index = factor_sum.index, columns = factor_sum.columns)
        weight_last = pd.Series(0, index = factor_sum.columns)

        for date in factor_sum.index:
            vol_control_daily = (self.vol_long_term.loc[date] / 2) + 1.5   #??2和1.5的设置
            hold_weight.loc[date] = self.hold_weight_daily(factor_sum.loc[date], weight_last, vol_control_daily)
            # weight_last = hold_weight.loc[date]
            print(date)
        return hold_weight
    # 根据复合因子值在一定风险敞口控制下生成当天的持仓权重
    def hold_weight_daily(self, factor_sum_daily, weight_last, vol_control_daily, long_short_max = 0.1, code_weight_max = 0.05, cate_exposure = 0.1, trans_costs = 0):      #??参数的具体释义
        code_list_all = factor_sum_daily.index.tolist()
        code_list = (factor_sum_daily+vol_control_daily).dropna().index.tolist()
        # 对象实例化（最大值）
        prob = LpProblem('myPro', LpMaximize)
        # prob = LpProblem('myPro', LpMinimize)   # 将最大值转换为最小值处理（便于后面剔除目标函数中的绝对值号）

        # 创建变量
        u = pd.Series(index=code_list)
        v = pd.Series(index=code_list)
        p = pd.Series(index=code_list)
        q = pd.Series(index=code_list)
        for code in code_list:
            u.loc[code] = LpVariable("%s_u" % code, lowBound=0)
            v.loc[code] = LpVariable("%s_v" % code, lowBound=0)
            p.loc[code] = LpVariable("%s_p" % code, lowBound=0)
            q.loc[code] = LpVariable("%s_q" % code, lowBound=0)


        # 设置目标函数
        objectFun = 0
        # 转换为求最小值，关于绝对值的处理，参考文献：徐伟宣，目标函数带绝对值号的特殊非线性规划问题
        for code in code_list:
            objectFun += (u.loc[code] - v.loc[code] + weight_last.loc[code]) * factor_sum_daily.loc[
                code] - trans_costs * (u.loc[code] + v.loc[code]) / 2
        #            objectFun += transactionCosts * (u.loc[stockCode] + v.loc[stockCode]) / 2 - (u.loc[stockCode] - v.loc[stockCode] + weight_Last.loc[stockCode]) * returnPred.loc[stockCode]
        prob += objectFun

        # 约束条件一，控制单只持仓权重最大值（考虑波动率）
        for code in code_list:
            prob += u.loc[code] - v.loc[code] + weight_last.loc[code] <= (code_weight_max/vol_control_daily.loc[code])
            prob += u.loc[code] - v.loc[code] + weight_last.loc[code] >= (-code_weight_max/vol_control_daily.loc[code])

        # 约束条件二，板块多空控制
        for cate in self.cate_info['category'].drop_duplicates():
            cate_code_list = self.cate_info[self.cate_info['category'] == cate]['code'].tolist()
            temp_WeightSum = 0
            holdingWeight = u - v + weight_last
            for code in code_list:
                if code in cate_code_list:
                    temp_WeightSum += holdingWeight.loc[code]
            prob += temp_WeightSum <= cate_exposure
            prob += temp_WeightSum >= -cate_exposure

        # 约束条件二，每期所有多空仓位控制
        temp_WeightSum = 0
        holdingWeight = u - v + weight_last
        for code in code_list:
            temp_WeightSum += holdingWeight.loc[code]
        prob += temp_WeightSum <= long_short_max
        prob += temp_WeightSum >= -long_short_max


        for code in code_list:
            prob += p.loc[code] - q.loc[code] == u.loc[code] - v.loc[code] + weight_last.loc[code]

        # 约束条件六，多空仓位绝对值相加为2
        temp_WeightSum = 0
        holdingWeight = p + q
        for code in code_list:
            temp_WeightSum += holdingWeight.loc[code]
        prob += temp_WeightSum == 2

        # 求解线性规划模型
        prob.solve()

        # 结果整理
        result_Dict = {}
        for i in prob.variables():
            result_Dict[str(i)] = i.varValue
        result_Series = pd.Series(result_Dict)

        weight = pd.Series(index=code_list)
        for code in code_list:
            u_Code = code + '_u'
            v_Code = code + '_v'
            weight.loc[code] = result_Series.loc[u_Code] - result_Series.loc[v_Code] + weight_last.loc[code]

        return weight.reindex(index = code_list_all).fillna(0)


    #############################################################################

    # 基金业绩归因的因子值相关度分析
    def fund_attribution(self, show = True):
        factor_ret_all = self.weekly_factor_report()
        abs_dir = 'C:/Users/Administrator/Desktop/workspace'
        net_value_data = pd.read_excel(abs_dir + '/绩效评估/CTA策略报告模板/1&2&3.产品规模总览&净值分析&风险分析.xlsx')
        fund_daily_ret = net_value_data[['日期', '日收益率']].set_index('日期').replace(0, np.nan).dropna()
        reg_Data = pd.concat([fund_daily_ret, factor_ret_all], axis=1).dropna()
        y_Var = np.array(reg_Data.iloc[:, 0])
        x_Var = sm.add_constant(np.array(reg_Data.iloc[:, 1:]))
        result = sm.OLS(y_Var, x_Var).fit()
        print(result.summary())
        fund_attribute = pd.Series(result.params[1:], index=factor_ret_all.columns)

        if show:
            fund_attribute_df = pd.DataFrame(fund_attribute, columns=['相关度'])
            fund_attribute_df['因子'] = fund_attribute_df.index
            plt.clf()
            fig = plt.figure(figsize=(10, 6))
            sns.barplot('因子', '相关度', data=fund_attribute_df)
            plt.rcParams['axes.unicode_minus'] = False
            # plt.gca().yaxis.set_major_formatter(PercentFormatter(xmax=1, decimals=0))
            plt.show()

            ret_all = factor_ret_all.copy()
            ret_all['xx基金'] = fund_daily_ret
            net_value = np.cumprod(1 + ret_all.dropna())
            plt.clf()
            fig = plt.figure(figsize=(10, 6))
            sns.lineplot(data=net_value)
            plt.show()

        return fund_attribute

    # 基金业绩归因的仓位相关度分析
    def position_analysis(self, show=True, drop_exchange = ['CFFEX']):
        abs_dir = 'C:/Users/Administrator/Desktop/workspace'
        position_all = pd.read_excel(abs_dir + '/绩效评估/客户数据/持仓汇总.xlsx')
        position_all['交易日'] = position_all['交易日'].apply(lambda x: pd.Timestamp(str(x)))
        position_all['交易所'] = position_all['交易所'].apply(lambda x: x.replace(' ', ''))
        date_list = position_all['交易日'].drop_duplicates().sort_values().tolist()

        code_info = pd.read_excel(abs_dir + '/绩效评估/code_info.xlsx')
        code_info = code_info[code_info['exchange'].apply(lambda x: x not in drop_exchange)]
        contract2code = lambda s: ''.join(x for x in s if x.isalpha())
        position_all['代码'] = position_all['合约'].apply(contract2code)

        factor_name_list = ['短期截面动量', '长期截面动量', '展期收益率', '基差动量', '偏度', '波动率']
        factor_exposure_df = pd.DataFrame(index = date_list, columns = factor_name_list)
        self.cal_factor_all()
        for factor_name in factor_name_list:
            factor_df = self.factor_all_dict[factor_name]
            factor_df_standard = factor_df.apply(lambda x : self.standardize(self.winsorize(x.dropna(), 3)).sort_values(), axis=1)
            for date in date_list:
                position_daily = position_all[position_all['交易日']==date]
                position_daily = position_daily[
                    position_daily['代码'].apply(lambda x: x in code_info['code'].to_list())]
                position_daily['持仓方向'][position_daily['持仓方向'] == 2] = 1
                position_daily['持仓方向'][position_daily['持仓方向'] == 3] = -1
                position_daily['市值'] = position_daily['今结算价'] * position_daily['数量'] * position_daily['合约乘数'] * position_daily['持仓方向']
                mkt_value = position_daily[['代码', '市值']].groupby('代码').sum()

                # position_ratio = (mkt_value/mkt_value.sum())['市值']*position_daily[['代码','持仓方向']].set_index('代码')['持仓方向']
                position_ratio = (mkt_value / mkt_value.abs().sum())['市值']
                factor_exposure_df.loc[date, factor_name] = (factor_df_standard.loc[date] * position_ratio).dropna().sum()
        if show:
            plt.clf()
            sns.lineplot(data=factor_exposure_df.astype(float))
            plt.rcParams['axes.unicode_minus'] = False
            plt.show()

        return factor_exposure_df


    ###########################################################################

    def time_series_momentum(self,show=True,rolling_window=5,test_date=None):
        import warnings
        warnings.filterwarnings("ignore")
        daily_ret_close = self.daily_ret_all_close.dropna(thresh=2).reindex(columns=self.trade_code_list)
        cum_net_value=(daily_ret_close+1).rolling(rolling_window).apply(lambda x:x.prod())
        ret_open=self.daily_ret_all_open

        long_list=[]
        short_list=[]

        for date in cum_net_value.index.tolist()[:-1]:
            date_next=cum_net_value.index.tolist()[cum_net_value.index.tolist().index(date)+1]
            long_category_list=cum_net_value.loc[date,:][cum_net_value.loc[date,:]>1].index.tolist()
            short_category_list = cum_net_value.loc[date, :][cum_net_value.loc[date, :] <= 1].index.tolist()

            if len(long_category_list) == 0:
                long_ret = 0
            else:
                with warnings.catch_warnings(record=True) as w:  # 消除RunningWarning
                    long_ret = np.mean(ret_open.loc[date_next, long_category_list].values)
                    if len(w) > 0:
                        long_ret = np.nan

            if len(short_category_list) == 0:
                short_ret = 0
            else:
                with warnings.catch_warnings(record=True) as w:
                    short_ret = np.mean(ret_open.loc[date_next, short_category_list].values)
                    if len(w) > 0:
                        short_ret = np.nan

            long_list.append(long_ret)
            short_list.append(short_ret)

        long_series = pd.Series(long_list, index=cum_net_value.index.tolist()[1:])
        short_series = pd.Series(short_list, index=cum_net_value.index.tolist()[1:])
        na_ret = (long_series - short_series).dropna()

        if test_date != None:
            na_ret = na_ret.loc[pd.Timestamp(test_date):]

        net_value = np.cumprod(1 + na_ret)
        if show:
            plt.clf()
            plt.plot(net_value)
            plt.show()
        return [na_ret, net_value]  # 第一个为时序回报率；第二个为累积净值


    def stock_index_futures_time_series_momentum(self,show=True,rolling_window=5,test_date=None):
        import warnings
        warnings.filterwarnings("ignore")
        daily_ret_close=self.daily_ret_all_close.dropna(thresh=2).reindex(columns=self.stock_index_futures_code_list)
        cum_net_value = (daily_ret_close + 1).rolling(rolling_window).apply(lambda x: x.prod())
        ret_open = self.daily_ret_all_open

        long_list = []
        short_list = []

        for date in cum_net_value.index.tolist()[:-1]:
            date_next = cum_net_value.index.tolist()[cum_net_value.index.tolist().index(date) + 1]
            long_category_list = cum_net_value.loc[date, :][cum_net_value.loc[date, :] > 1].index.tolist()
            short_category_list = cum_net_value.loc[date, :][cum_net_value.loc[date, :] <= 1].index.tolist()

            if len(long_category_list)==0:
                long_ret=0
            else:
                with warnings.catch_warnings(record=True) as w:  # 消除RunningWarning
                    long_ret = np.mean(ret_open.loc[date_next, long_category_list].values)
                    if len(w) > 0:
                        long_ret = np.nan

            if len(short_category_list)==0:
                short_ret=0
            else:
                with warnings.catch_warnings(record=True) as w:
                    short_ret = np.mean(ret_open.loc[date_next, short_category_list].values)
                    if len(w) > 0:
                        short_ret = np.nan

            long_list.append(long_ret)
            short_list.append(short_ret)

        long_series = pd.Series(long_list, index=cum_net_value.index.tolist()[1:])
        short_series = pd.Series(short_list, index=cum_net_value.index.tolist()[1:])
        na_ret = (long_series - short_series).dropna()

        if test_date != None:
            na_ret = na_ret.loc[pd.Timestamp(test_date):]

        net_value = np.cumprod(1 + na_ret)
        if show:
            plt.clf()
            plt.plot(net_value)
            plt.show()
        return [na_ret, net_value]  # 第一个为时序回报率；第二个为累积净值


    def treasury_index_futures_time_series_momentum(self,show=True,rolling_window=5,test_date=None):
        import warnings
        warnings.filterwarnings("ignore")
        daily_ret_close = self.daily_ret_all_close.dropna(thresh=2).reindex(columns=self.treasury_index_futures_code_list)
        cum_net_value = (daily_ret_close + 1).rolling(rolling_window).apply(lambda x: x.prod())
        ret_open = self.daily_ret_all_open

        long_list = []
        short_list = []

        for date in cum_net_value.index.tolist()[:-1]:
            date_next = cum_net_value.index.tolist()[cum_net_value.index.tolist().index(date) + 1]
            long_category_list = cum_net_value.loc[date, :][cum_net_value.loc[date, :] > 1].index.tolist()
            short_category_list = cum_net_value.loc[date, :][cum_net_value.loc[date, :] <= 1].index.tolist()

            if len(long_category_list) == 0:
                long_ret = 0
            else:
                with warnings.catch_warnings(record=True) as w:  # 消除RunningWarning
                    long_ret = np.mean(ret_open.loc[date_next, long_category_list].values)
                    if len(w) > 0:
                        long_ret = np.nan

            if len(short_category_list) == 0:
                short_ret = 0
            else:
                with warnings.catch_warnings(record=True) as w:
                    short_ret = np.mean(ret_open.loc[date_next, short_category_list].values)
                    if len(w) > 0:
                        short_ret = np.nan

            long_list.append(long_ret)
            short_list.append(short_ret)

        long_series = pd.Series(long_list, index=cum_net_value.index.tolist()[1:])
        short_series = pd.Series(short_list, index=cum_net_value.index.tolist()[1:])
        na_ret = (long_series - short_series).dropna()

        if test_date != None:
            na_ret = na_ret.loc[pd.Timestamp(test_date):]

        net_value = np.cumprod(1 + na_ret)
        if show:
            plt.clf()
            plt.plot(net_value)
            plt.show()
        return [na_ret, net_value]  # 第一个为时序回报率；第二个为累积净值


    def zhaoshang_index_time_series_momentum(self,input_data,show=True,rolling_window=5,test_date=None):
        import warnings
        warnings.filterwarnings("ignore")
        input_data.drop(index=[0, 1], inplace=True)
        zhaoshang_index_value = input_data.iloc[1:, 1:4].values
        zhaoshang_index_index = [pd.Timestamp(x) for x in input_data.iloc[1:, 0].values.tolist()]
        zhaoshang_index_data = pd.DataFrame(zhaoshang_index_value, index=zhaoshang_index_index,columns=['CI013001.WI', 'CI013004.WI', 'CI013002.WI']).sort_index()
        zhaoshang_index_data.index.name = '日期'

        zhaoshang_index_pct=zhaoshang_index_data.pct_change().dropna()

        na_ret=np.mean(zhaoshang_index_pct,axis=1)

        if test_date != None:
            na_ret = na_ret.loc[test_date]

        net_value = np.cumprod(1 + na_ret)
        if show:
            plt.clf()
            plt.plot(net_value)
            plt.show()
        return [na_ret, net_value]  # 第一个为时序回报率；第二个为累积净值


    def liquidity_ILLIQ(self,rolling_window=20):
        import warnings
        warnings.filterwarnings("ignore")
        daily_ret_close=self.daily_ret_all_close
        daily_turnover=self.daily_main_contract_turnover_all
        date_time_list=list(np.sort(list(set(daily_ret_close.index.tolist()).intersection(set(daily_turnover.index.tolist())))))
        factor=pd.DataFrame(index=date_time_list,columns=self.code_info.index.tolist())
        for date in date_time_list:
            for code in self.code_info.index:
                if daily_turnover.loc[date,code]==0:
                    factor.loc[date,code]=100000000
                elif daily_turnover.loc[date,code]==np.nan:
                    factor.loc[date,code]=np.nan
                else:
                    factor.loc[date, code] = (np.abs(daily_ret_close.loc[date, code]) / daily_turnover.loc[date, code]) * 100000000

        factor=factor.rolling(rolling_window).mean()

        return factor


















