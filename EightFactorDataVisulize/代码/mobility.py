import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
from matplotlib.font_manager import FontProperties
from matplotlib.ticker import PercentFormatter
import datetime
import cx_Oracle
import openpyxl
import os
#  调色
myfont=FontProperties(fname=r'C:\Windows\Fonts\simhei.ttf',size=14)
sns.set(font=myfont.get_name(), palette = 'Oranges_r', style = 'white')
color_pa_5 = ['#ff5b00', '#ffb07c', '#a83c09', '#978a84', '#411900']
'''连接数据库'''
tns = cx_Oracle.makedsn('10.243.73.69', 1521, 'orcl')
db = cx_Oracle.connect('QIHUO_CTP_APP', 'QIHUO_CTP_APP', tns)

def main(_end_date,_last_week):
    cursor = db.cursor()
    end_date = _end_date
    start_date = str(int(end_date[:4]) - 4) + end_date[4:]  # start_date 取4年
    save_dir = './流动性/'
    last_week = _last_week
    date1 = datetime.datetime.strptime(end_date, "%Y%m%d")
    date2 = datetime.datetime.strptime(last_week, "%Y%m%d")
    days_count = (date2 - date1).days

    '''动量 波动率等存在于同一个表格不需要读'''
    tmp_df = pd.read_excel("配置5.xlsx",sheet_name="期货价格")
    momentum_list = []
    for i in range(len(tmp_df)):
        tuple_x = tmp_df.iloc[i, :]
        tmp_name = tuple_x['f_name']  # 取出名字
        momentum_list.append(tmp_name)

    '''日期'''
    dates = pd.date_range(start_date, end_date)
    dates_next = []  # 创造日期的索引
    for date_x in dates:
        date_x = date_x.strftime('%Y%m%d')
        dates_next.append(date_x)
    momentum_df = pd.DataFrame(index=dates_next)  # 库存四年数据

    '''数据库查询'''
    for momentum in momentum_list:
        tmp_dataname = '流动性'
        tmp_table = 'FACTOR_PAQHYJS_FEATURE'
        str_cur = ''' select F_VALUE,F_DATE from ''' + tmp_table + ''' where F_FEATURE =''' + """'""" + tmp_dataname + """'""" +  ''' and F_S_NAME =''' + """'""" + momentum + """'""" +''' and F_DATE>=''' + """'""" + start_date + """'""" + ''' and F_DATE<=''' + """'""" + end_date + """'"""
        print(str_cur)
        cursor = db.cursor()
        cursor.execute(str_cur)
        new_df = pd.DataFrame(data=list(cursor.fetchall()),columns=[momentum,'日期'])
        new_df = new_df.set_index('日期')
        momentum_df = momentum_df.join(new_df,how="outer")
        print(new_df)
    momentum_df = momentum_df.astype(float)
    '''去空，前值填充'''

    momentum_df.dropna(axis=1, inplace=True, how='all')  # 如果某一列全为空那么除去这些数据
    momentum_df.dropna(axis=0, inplace=True, how='all')  # 如果某一行全为空那么除去这些数据
    momentum_df.fillna(method='ffill')

    momentum_quantile_df = (momentum_df - momentum_df.min()) / (momentum_df.max() - momentum_df.min())
    quantile_sort = momentum_quantile_df[momentum_quantile_df.iloc[-1].sort_values().index]  # 取今日的数据行进行排序并且固定顺序
    quantile_sort_diff = (
            quantile_sort.iloc[-1] - quantile_sort.loc[last_week]).sort_values()  # diff是本日与上周五交易日的差并sort
    quantile_sort_dev = (quantile_sort.iloc[-1] - quantile_sort.median()).sort_values()  # dev是本日与1000日以来平均数的差的排序
    top_diff = quantile_sort_diff[quantile_sort_diff > 0].index[-3:].to_list()  # 找到三款与上周相比，值增加并倒序
    bottom_diff = quantile_sort_diff[quantile_sort_diff < 0].index[:3].to_list()  # 找出三款亏了
    top_dev = quantile_sort_dev[
                  quantile_sort_dev > quantile_sort.quantile(0.75).reindex(quantile_sort_dev.index)].index[
              -3:].to_list()
    bottom_dev = quantile_sort_dev[
                     quantile_sort_dev < quantile_sort.quantile(0.25).reindex(quantile_sort_dev.index)].index[
                 :3].to_list()
    # top_diff求分位值 dev极端分位
    top_dev.reverse()
    top_diff.reverse()

    diff_txt_list = ['过去一周分位值上升靠前：' + str(top_diff).replace('[', '').replace(']', '').replace("'", ''),
                     '过去一周分位值下降靠前：' + str(bottom_diff).replace('[', '').replace(']', '').replace("'", '')]
    dev_txt_list = ['相对过去四年处于75%高分位：' + str(top_dev).replace('[', '').replace(']', '').replace("'", ''),
                    '相对过去四年处于25%低分位：' + str(bottom_dev).replace('[', '').replace(']', '').replace("'", '')]

    '''写入文档'''
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.append(["流动性"])
    ws.append(diff_txt_list)
    ws.append(dev_txt_list)
    file_name = save_dir  + '流动性'
    try:
        os.remove(file_name)
    except:
        pass
    wb.save(save_dir  + '流动性' + '.xlsx')

    '''画图'''
    # 由于图较大，分成了2个部分来做这个图
    half_num = round(len(quantile_sort.columns) / 2)
    quantile_sort_top = quantile_sort.iloc[:, :half_num]
    plt.clf()
    title = '流动性过去4年分位-箱线密度图-1'
    fig = plt.figure(figsize=(16, 16))
    sns.violinplot(data=quantile_sort_top, orient='h')
    plt.plot(quantile_sort_top.loc[last_week], quantile_sort_top.columns, 'bD')
    plt.plot(quantile_sort_top.iloc[-1], quantile_sort_top.columns, 'rs')
    plt.rcParams['axes.unicode_minus'] = False
    plt.gca().xaxis.set_major_formatter(PercentFormatter(xmax=1, decimals=0))
    ax = plt.gca()
    ax.tick_params(labelright=True)
    plt.title(title, fontsize=30)
    plt.xticks(fontsize=30)
    plt.yticks(fontsize=20)

    y = np.arange(len(quantile_sort_top.columns))
    x = np.array(list(quantile_sort_top.iloc[-1]))
    z = np.array(list(momentum_df[quantile_sort_top.columns].iloc[-1]))

    for a, b, c in zip(x, y, z):
        plt.text(a, b - 0.1,('%.2f' % c) , ha='center', va='bottom', fontsize=15,
                 bbox=dict(ec='black', fc='white', alpha=0.7))

    plt.savefig(save_dir + title + '.png', bbox_inches='tight')
    plt.show()

    half_num = round(len(quantile_sort.columns) / 2)
    quantile_sort_bottom = quantile_sort.iloc[:, half_num:]
    plt.clf()
    title = '流动性过去4年分位-箱线密度图-2'
    fig = plt.figure(figsize=(16, 16))
    sns.violinplot(data=quantile_sort_bottom, orient='h')
    plt.plot(quantile_sort_bottom.loc[last_week], quantile_sort_bottom.columns, 'bD')
    plt.plot(quantile_sort_bottom.iloc[-1], quantile_sort_bottom.columns, 'rs')
    plt.rcParams['axes.unicode_minus'] = False
    plt.gca().xaxis.set_major_formatter(PercentFormatter(xmax=1, decimals=0))
    plt.title(title, fontsize=30)
    ax = plt.gca()
    ax.tick_params(labelright=True)
    plt.xticks(fontsize=30)
    plt.yticks(fontsize=20)

    y = np.arange(len(quantile_sort_bottom.columns))
    x = np.array(list(quantile_sort_bottom.iloc[-1]))
    z = np.array(list(momentum_df[quantile_sort_bottom.columns].iloc[-1]))

    for a, b, c in zip(x, y, z):
        plt.text(a, b-0.1 , ('%.2f' % c), ha='center', va='bottom', fontsize=15,
                 bbox=dict(ec='black', fc='white', alpha=0.7))

    plt.savefig(save_dir + title + '.png', bbox_inches='tight')
    plt.show()


if __name__ =='__main__':
    main()