import copy
from collections import defaultdict
import pandas as pd
from pyecharts import options as opts
from pyecharts.charts import Bar
from pyecharts.render import make_snapshot

from snapshot_selenium import snapshot
import numpy as np
import time
import datetime
import os,stat
import seaborn as sns
import matplotlib.pyplot as plt
from matplotlib.font_manager import FontProperties
from matplotlib.ticker import PercentFormatter
import cx_Oracle
from pyecharts.render import make_snapshot
from pyecharts.charts import Page,TreeMap
from pyecharts import options as opts
# pyecharts安装:$ pip install pyecharts -U

#  调色
myfont=FontProperties(fname=r'C:\Windows\Fonts\simhei.ttf',size=14)
sns.set(font=myfont.get_name(), palette = 'Oranges_r', style = 'white')
color_pa_5 = ['#ff5b00', '#ffb07c', '#a83c09', '#978a84', '#411900']
#category_teams= {"t_scsj_mysteel_data":"黑色","t_scsj_mymetal_data":"有色","t_scsj_mychemi_data":"化工","t_scsj_myagric_data":"农产品"}
maps = {'螺纹钢':"黑色",
'热卷':"黑色",
'热轧卷板':"黑色",
'锰硅':"黑色",
'焦煤':"黑色",
'焦炭':"黑色",
'动力煤':"黑色",
'铁矿石':"黑色",
'硅铁':"黑色",
'硅锰':"黑色",
'铜':'有色',
'铝':'有色',
'锌':'有色',
'镍':'有色',
'不锈钢':'有色',
'PTA':'化工',
'乙二醇':'化工',
'短纤':'化工',
'PVC':'化工',
'苯乙烯':'化工',
'玻璃':'化工',
'纯碱':'化工',
'LPG':'化工',
'聚丙烯':'化工',
'天然橡胶':'化工',
'甲醇':'化工',
'塑料':'化工',
'PP':'化工',
'尿素':'化工',
'沥青':'化工',
'纸浆':'化工',
'橡胶':'化工',
'玉米':'农业',
'淀粉':'农业',
'豆粕':'农业',
'豆油':'农业',
'棕榈油':'农业',
'菜粕':'农业',
'菜油':'农业',
'白糖':'农业',
'苹果':'农业',
'生猪':'农业',
'红枣':'农业',
'棉花':'农业',
'黄玉米':'农业',
'菜籽粕':'农业',
'菜籽油':'农业',
'花生':'农业',
'白砂糖':'农业',
'鸡蛋':'农业',
'玉米淀粉':'农业',
'黄金':'有色',
'阴极铜':"有色",
 '白银':'有色',
    '锡':'有色'}


'''连接数据库'''
tns = cx_Oracle.makedsn('10.243.73.69', 1521, 'orcl')
db = cx_Oracle.connect('QIHUO_CTP_APP', 'QIHUO_CTP_APP', tns)
def main(_end_date,_last_week):
    # 数据准备
    start_date = _last_week
    end_date = _end_date
    df = pd.read_excel("../配置/配置.xlsx",sheet_name="期货价格")
    save_dir = os.getcwd() + '/' + end_date + '/热力图/'
    if not os.path.exists(save_dir):
        os.makedirs(save_dir)
        os.chmod(save_dir, stat.S_IWOTH)
    cursor = db.cursor()
    length = len(df) #获取需要读取的价格数据长度
    cate_percent = []
    for i in range(len(df)):
        percent = 0
        tuple_x = df.iloc[i,:] #取第一行的数据 以tuple的形式
        tmp = tuple_x['f_name']#期货物品
        table_name = str(tuple_x['db_table']) #获取查询的表格名字
        #tmp_cate = category_teams[table_name] #期货种类
        tmp_cate = maps[tmp]
        data_name = str(tuple_x['data_name'])#查询的dataname

        str_start = ''' select F_VALUE from ''' + table_name + ''' where F_DATANAME =''' + """'""" + data_name + """'""" + ''' and F_DATE=''' + """'""" + start_date + """'"""
        str_end = ''' select F_VALUE from ''' + table_name + ''' where F_DATANAME =''' + """'""" + data_name + """'""" + ''' and F_DATE=''' + """'""" + end_date + """'"""

        try:
            cursor.execute(str_start)
            tmp_list = cursor.fetchall()
            start_val = float(tmp_list[0][0])#上一周的数据
            cursor.execute(str_end)
            tmp_list = cursor.fetchall()
            end_val = float(tmp_list[0][0])#这一周的数据
        except:
            print("未搜索到"+tmp+"在"+str(start_date))
            print("未搜索到" + tmp + "在" + str(end_date))
        try:
            percent = (end_val - start_val) / start_val * 100
            new_tuple = (tmp, tmp_cate, percent)#产品，种类，百分比
            cate_percent.append(new_tuple)
            print(start_val , end_val,tmp, tmp_cate, percent)

        except:
            print("无法计算"+ tmp + "热力图，请检查输入")


    # 热力图

    '''找寻父节点'''
    def find_father(nums,tar_str):
        for i,num in enumerate(nums):
            if num["name"] == tar_str:
                return i
        return -1

    '''根据数值返回颜色'''
    def find_color(val):
        #涨
        if val > 2.00:return "涨",'rgb(139, 0, 0)'
        elif val > 1.00 and val <= 2.00:return "涨",'rgb(255, 0, 0)'
        elif val > 0.5 and val <= 1.00:return "涨", 'rgb(255,48,48)'
        elif val > 0 and val <= 0.5:return "涨", 'rgb(255,106,106)'
        #跌
        elif val < 0 and val >= -0.5 :return "跌", 'rgb(144,238,144)'
        elif val < -0.5 and val >= -1.0 :return "跌", 'rgb(0, 255, 127)'
        elif val < -1.0 and val >= -2.0:return "跌",'rgb(0, 255, 0)'
        elif val < -2.0 :return "跌",'rgb(0, 139, 0)'
        #不变
        else:return "波动较小",'rgb(128, 128, 128)'

    title = '商品指数周涨跌幅热力图（分板块）'
    tree = []
    pieces = []#分层
    for tran,cate_x in enumerate(cate_percent):
        children_x = {}
        children_x["value"] = abs(cate_x[2]) + 0.00000000000001 * tran#录入成正数
        new_val = round(cate_x[2],1) #new_val代表他的名义数值
        raw_val = cate_x[2] #raw_Val指的是他不加修改的值
        #children_x["value"] = round((cate_x[2]), 2)
        abs_val = abs(new_val)
        raw_val = children_x["value"]
        if new_val < 0:children_x["name"]= cate_x[0] + '\n' + '-' +str(abs_val) + '%'
        else:children_x["name"]= cate_x[0] + '\n' + str(abs_val) + '%'

        flag,new_color = find_color(new_val)
        pieces.append({"min":abs(raw_val),"max":abs(raw_val),"label":children_x["name"]+flag+str(abs(new_val))+'%',"color":new_color })
        #tmp_tree_item = opts.TreeItem(name =children_x["name"],value=children_x["value"],itemstyle_opts=opts.ItemStyleOpts(color=find_color(cate_x[2])) )
        #children_x["label_opts"] = opts.LabelOpts(color=find_color(cate_x[2]))

        if not tree or find_father(tree,cate_x[1]) == -1:
            tree.append({'value':round(cate_x[2],1),"name":cate_x[1] ,"children":[copy.deepcopy(children_x)]})
        else:
            tar_list = tree[find_father(tree,cate_x[1])]
            tar_list['value'] += children_x["value"]
            tar_list['children'].append(copy.deepcopy(children_x))
    tm = (
        TreeMap()
            .add(end_date,data = tree,color_saturation=[0,0.5],width='90%',height='90%' )
            .set_global_opts(visualmap_opts=opts.VisualMapOpts(is_show=False,is_piecewise=True, pieces=pieces)
            ,title_opts=opts.TitleOpts(title='商品指数周涨跌幅热力图（分板块）', subtitle='平安期货')
        )
    )
    tm.render( save_dir + '热力图.html')
    make_snapshot(snapshot, tm.render(), save_dir + "热力图.png")


if __name__ == "__main__":
    main("20221110","20221103")



