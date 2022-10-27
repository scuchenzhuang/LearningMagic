import os,stat
import openpyxl
import pandas as pd
import numpy as np
import seaborn as sns
import datetime
import matplotlib.pyplot as plt
from matplotlib.font_manager import FontProperties
from matplotlib.ticker import PercentFormatter
import cx_Oracle
#  调色
myfont=FontProperties(fname=r'C:\Windows\Fonts\simhei.ttf',size=14)
sns.set(font=myfont.get_name(), palette = 'Oranges_r', style = 'white')
color_pa_5 = ['#ff5b00', '#ffb07c', '#a83c09', '#978a84', '#411900']
'''连接数据库'''
tns = cx_Oracle.makedsn('10.243.73.69', 1521, 'orcl')
db = cx_Oracle.connect('QIHUO_CTP_APP', 'QIHUO_CTP_APP', tns)

def main(_end_date,_last_week):
    end_date = _end_date
    last_week = _last_week  # 上一周的数据对应的值
    start_date = str(int(end_date[:4]) - 4) + end_date[4:]  # start_date 取4年

    save_dir = os.getcwd()+'/' + end_date + '/基差率/'
    if not os.path.exists(save_dir):
        os.makedirs(save_dir)
        os.chmod(save_dir, stat.S_IWOTH)
    '''读取配置文档'''
    tmp_df1 = pd.read_excel("../配置/配置.xlsx",sheet_name="现货价格")
    tmp_df2 = pd.read_excel("../配置/配置.xlsx", sheet_name="基差")
    cur_map = {} #现货价格查询映射  螺纹钢:(xxx价格，xx表)
    diff_map = {}#基差查询映射
    for i in range(len(tmp_df1)):
        tuple_x = tmp_df1.iloc[i, :]  # 取第一行的数据 以tuple的形式
        tmp_name = tuple_x['f_name']  # 期货物品名字
        tmp_dataname = tuple_x['data_name']
        tmp_table = tuple_x['db_table']
        cur_map[tmp_name] = (tmp_dataname,tmp_table)
    for i in range(len(tmp_df2)):
        tuple_x = tmp_df2.iloc[i, :]  # 取第一行的数据 以tuple的形式
        tmp_name = tuple_x['f_name']  # 期货物品名字
        tmp_dataname = tuple_x['data_name']
        tmp_table = tuple_x['db_table']
        diff_map[tmp_name] = (tmp_dataname,tmp_table)
    dates = pd.date_range(start_date,end_date)
    dates_next = []#创造日期的索引
    for date_x in dates:
        date_x = date_x.strftime('%Y%m%d')
        dates_next.append(date_x)
    spread_cur_df = pd.DataFrame(index=dates_next)#现货价格4年数据
    spread_diff_df = pd.DataFrame(index=dates_next)#基差价格4年数据
    for cur in cur_map:
        #现货的db相关查询
        tmp_dataname = cur_map[cur][0]
        tmp_table = cur_map[cur][1]
        str_cur = ''' select ID,F_DATE,F_VALUE from ''' + tmp_table + ''' where F_DATANAME =''' + """'""" + tmp_dataname + """'""" + ''' and F_DATE>=''' + """'""" + start_date + """'""" + ''' and F_DATE<=''' + """'""" +end_date +"""'""" +""" group by F_DATE,ID,F_VALUE order by F_DATE"""

        try:
            cursor = db.cursor()
            cursor.execute(str_cur)
            tmp_list = list(cursor.fetchall())

            tmp_map = {}
            # 即取最后一次更新的数据，目前认为库存和利润重复数据的原因是由于更新，那么我们计算时主要处理
            for tuple_y in tmp_list:
                if tuple_y[1] not in tmp_map:
                    tmp_map[tuple_y[1]] = [tuple_y[0], tuple_y[2]]
                else:
                    if tuple_y[0] > tmp_map[tuple_y[1]][0]: tmp_map[tuple_y[1]] = [tuple_y[0], tuple_y[2]]
            after_list = []
            for keys in tmp_map:
                after_list.append((keys, tmp_map[keys][1]))
            after_list = sorted(after_list, key=lambda x: x[0])
            print(after_list)
            new_df = pd.DataFrame(data=after_list, columns=['日期', cur])
            new_df = new_df.set_index('日期')
            spread_cur_df =  spread_cur_df.join(new_df, how="outer")
        except:
            print("在查找现货时出错，item是" + cur)


        #基差的db相关查询
        tmp_dataname = diff_map[cur][0]
        tmp_table = diff_map[cur][1]
        str_diff = ''' select F_VALUE,F_DATE from ''' + tmp_table + ''' where F_DATANAME =''' + """'""" + tmp_dataname + """'""" + ''' and F_DATE>=''' + """'""" + start_date + """'""" + ''' and F_DATE<=''' + """'""" + end_date + """'"""
        try:
            cursor = db.cursor()
            cursor.execute(str_diff)
            new_diff_df = pd.DataFrame(data=list(cursor.fetchall()), columns=[cur, '日期'])
            new_diff_df = new_diff_df.set_index('日期')
            spread_diff_df = spread_diff_df.join(new_diff_df, how="outer")
        except:
            print("在查找基差时出错，item是" + cur)
    '''单位换算'''
    spread_cur_df['苹果'] = spread_cur_df['苹果']*2000
    spread_cur_df['红枣'] = spread_cur_df['红枣'] * 2000
    spread_cur_df['生猪'] = spread_cur_df['生猪'] * 2000
    spread_cur_df['铁矿石'] = spread_cur_df['铁矿石']  / 0.91

    spread_df = spread_diff_df / spread_cur_df  # 基差/现货价格 = 基差率 = (期货-现货)/现货
    spread_df = spread_df.astype(float)
    '''按照inf和0置空 前值填充'''

    spread_df.dropna(axis=1,inplace=True,how='all') #如果某一列全为空那么除去这些数据
    spread_df.dropna(axis=0, inplace=True, how='all')  # 如果某一行全为空那么除去这些数据
    spread_df = spread_df.replace(0, np.nan).fillna(method='ffill')
    spread_df = spread_df.replace(np.inf, np.nan).fillna(method='ffill')

    '''数据归一化'''
    spread_df_quantile_df = (spread_df - spread_df.min()) / (spread_df.max() - spread_df.min())
    quantile_sort = spread_df_quantile_df[spread_df_quantile_df.iloc[-1].sort_values().index]#按照今天的基差率排序

    '''核心计算'''

    quantile_sort_diff = (quantile_sort.iloc[-1] - quantile_sort.loc[last_week].iloc[0]).sort_values() #与上周的数据进行相减
    quantile_sort_dev = (quantile_sort.iloc[-1] - quantile_sort.median()).sort_values()
    top_diff = quantile_sort_diff[quantile_sort_diff > 0].index[-3:].to_list()
    bottom_diff = quantile_sort_diff[quantile_sort_diff < 0].index[:3].to_list()
    #top_dev = quantile_sort_dev[quantile_sort_dev > quantile_sort.quantile(0.75).reindex(quantile_sort_dev.index)].index[-3:].to_list()

    top_dev = quantile_sort_dev[
                  quantile_sort_dev > quantile_sort.quantile(0.75).reindex(quantile_sort_dev.index)].index.to_list()
    bottom_dev = quantile_sort_dev[
                     quantile_sort_dev < quantile_sort.quantile(0.25).reindex(quantile_sort_dev.index)].index.to_list()
    top_dev.reverse()
    top_diff.reverse()
    '''
    diff_txt = '过去一周分位值上升靠前：' + str(top_diff).replace('[', '').replace(']', '').replace("'", '') + '\n' \
               + '过去一周分位值下降靠前：' + str(bottom_diff).replace('[', '').replace(']', '').replace("'", '')+'\n'
    dev_txt = '相对过去四年处于75%高分位：' + str(top_dev).replace('[', '').replace(']', '').replace("'", '') + '\n' \
              + '相对过去四年处于25%低分位：' + str(bottom_dev).replace('[', '').replace(']', '').replace("'", '')
    '''
    diff_txt_list = ['过去一周分位值上升靠前：' , str(top_diff).replace('[', '').replace(']', '').replace("'", ''),'过去一周分位值下降靠前：' , str(bottom_diff).replace('[', '').replace(']', '').replace("'", '')]
    dev_txt_list = ['相对过去四年处于75%高分位：' , str(top_dev).replace('[', '').replace(']', '').replace("'", '') ,'相对过去四年处于25%低分位：' , str(bottom_dev).replace('[', '').replace(']', '').replace("'", '')]



    '''画图'''
    plt.clf()
    title = '基差率过去4年分位-箱线密度图'
    fig = plt.figure(figsize=(16, 16))


    #quantile_sort.fillna('-')
    #violins = sns.violinplot(data=quantile_sort[quantile_sort!='-'], orient='h',linewidth=0.5)
    violins = sns.violinplot(data=quantile_sort, orient='h',linewidth=0.5)
    plt.plot(quantile_sort.loc[last_week], quantile_sort.columns, 'bD')
    plt.plot(quantile_sort.iloc[-1], quantile_sort.columns, 'rs',markersize=13)
    plt.rcParams['axes.unicode_minus'] = False
    plt.gca().xaxis.set_major_formatter(PercentFormatter(xmax=1, decimals=0))
    ax = plt.gca()
    ax.tick_params(labelright = True)
    plt.title(title, fontsize=30)
    plt.xticks(fontsize=30)
    plt.yticks(fontsize=20)
    y = np.arange(len(quantile_sort.columns))
    x = np.array(list(quantile_sort.iloc[-1]))
    z = np.array(list(spread_df[quantile_sort.columns].iloc[-1]))

    for a, b, c in zip(x, y, z):
        plt.text(a, b - 0.1, ('%.0f' % (c * 100)) + '%', ha='center', va='bottom', fontsize=15,
                 bbox=dict(ec='black', fc='white', alpha=0.7))


    plt.savefig(save_dir + title + '.png', bbox_inches='tight')
    plt.show()

    # 由于图较大，分成了2个部分来做这个图
    half_num = round(len(quantile_sort.columns) / 2)
    quantile_sort_top = quantile_sort.iloc[:, :half_num]
    plt.clf()
    title = '基差率过去4年分位-箱线密度图-1'
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
    z = np.array(list(spread_df[quantile_sort_top.columns].iloc[-1]))
    for a, b, c in zip(x, y, z):
        plt.text(a, b - 0.1, ('%.0f' % (c * 100)) + '%', ha='center', va='bottom', fontsize=15,
                 bbox=dict(ec='black', fc='white', alpha=0.7))

    plt.savefig(save_dir + title + '.png', bbox_inches='tight')
    plt.show()

    half_num = round(len(quantile_sort.columns) / 2)
    quantile_sort_bottom = quantile_sort.iloc[:, half_num:]
    plt.clf()
    title = '基差率过去4年分位-箱线密度图-2'
    fig = plt.figure(figsize=(16, 16))
    sns.violinplot(data=quantile_sort_bottom, orient='h')
    plt.plot(quantile_sort_bottom.loc[last_week], quantile_sort_bottom.columns, 'bD')
    plt.plot(quantile_sort_bottom.iloc[-1], quantile_sort_bottom.columns, 'rs')
    plt.rcParams['axes.unicode_minus'] = False
    plt.gca().xaxis.set_major_formatter(PercentFormatter(xmax=1, decimals=0))
    ax = plt.gca()
    ax.tick_params(labelright=True)
    plt.title(title, fontsize=30)
    plt.xticks(fontsize=30)
    plt.yticks(fontsize=20)

    y = np.arange(len(quantile_sort_bottom.columns))
    x = np.array(list(quantile_sort_bottom.iloc[-1]))
    z = np.array(list(spread_df[quantile_sort_bottom.columns].iloc[-1]))
    for a, b, c in zip(x, y, z):
        plt.text(a, b - 0.1, ('%.0f' % (c * 100)) + '%', ha='center', va='bottom', fontsize=15,
                 bbox=dict(ec='black', fc='white', alpha=0.7))

    plt.savefig(save_dir + title + '.png', bbox_inches='tight')
    plt.show()

    return [["基差率"],(diff_txt_list[0], diff_txt_list[1]),(diff_txt_list[2], diff_txt_list[3]),(dev_txt_list[0], dev_txt_list[1]),(dev_txt_list[2], dev_txt_list[3])]
    '''写入文档'''
    '''
    excel_name = os.getcwd() + '/' + end_date + '/' + end_date + '.xlsx'
    wb = openpyxl.load_workbook(filename=excel_name)
    ws = wb.active
    # pre_row = ws.max_row
    ws.append(["基差率"])
    ws.append((diff_txt_list[0], diff_txt_list[1]))
    ws.append((diff_txt_list[2], diff_txt_list[3]))
    ws.append((dev_txt_list[0], dev_txt_list[1]))
    ws.append((dev_txt_list[2], dev_txt_list[3]))
    wb.save(excel_name)
    cursor.close()
    '''
if __name__ == "__main__":
    print("开始计算基差率")
    main("20221021","20220930")
