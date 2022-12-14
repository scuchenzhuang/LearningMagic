import openpyxl
import pandas as pd
import 基差率_new as jcl
import 热力图_new as rlt
import 库存_new as kc
import momentum_5d as m_5d
import momentum_20d as m_20d
import volatility as vol
import mobility as mob
import 价格_new as priceGraph
import rollyeid as rol
import 利润_new as profitGraph
import os,stat
import numpy as np
from sortedcontainers import SortedDict
import threading
from multiprocessing import Process
from multiprocessing import Manager
from multiprocessing import Pool
def main():
    '''此处修改今日日期，与之比较的上周数据'''
    '''今日'''
    end_date = "20221201"
    '''与上周的哪一日进行比较'''
    last_week = '20221124'


    path = os.getcwd()
    path += "/" + end_date
    file = path + '/' + end_date + '.xlsx'
    if not os.path.exists(path):
        os.makedirs(path)
        os.chmod(path, stat.S_IWOTH)
    if os.path.exists(file):
        os.unlink(file)
    wb = openpyxl.Workbook()
    sheet = wb.active
    sheet.title = end_date
    wb.save(file)

    '''动量 波动率等存在于同一个表格不需要读'''
    action_df = pd.read_excel("../配置/配置.xlsx", sheet_name="执行计划")
    action_list = []
    action_map = {}
    for i in range(len(action_df)):
        tuple_x = action_df.iloc[i, :]
        if not np.isnan(tuple_x['是否执行']) and tuple_x['是否执行'] == 1:
            action_map[tuple_x['因子名']] = tuple_x['顺序']
            action_list.append(tuple_x['因子名'])
    action_list = sorted(action_list,key = lambda x:action_map[x])
    print(action_map,action_list)

    '''多进程板块，多进程执行图形建设'''
    pool = Pool(processes=10) #创建进程池
    results = []
    def return_obj_main(s):
        if s == '基差率':return jcl.main
        elif s == '库存':return kc.main
        elif s == '5日动量':return m_5d.main
        elif s == '20日动量':return m_20d.main
        elif s == '波动率':return vol.main
        elif s == '流动性':return mob.main
        elif s == '价格':return priceGraph.main
        elif s == '利润':return profitGraph.main
        elif s == '展期收益率':return rol.main
        else:return None
    for action in action_list:
        func_x = return_obj_main(action)
        if func_x:
            results.append(pool.apply_async(func_x, (end_date,last_week)))
    if '热力图' in action_list:
        Process(target=rlt.main, args=(end_date, last_week)).start()
        #action_list.remove('热力图')
    #print(results)
    #Process(target=rlt.main, args=(end_date, last_week)).start()
    #写入文档
    wb = openpyxl.load_workbook(filename=file)
    ws = wb.active
    for res in results:
        try:
            tmplist = res.get()
            for tmp in tmplist:
                ws.append(tmp)
        except:
            print("存在执行出错的图片，请检查")
    wb.save(file)




if __name__ == '__main__':
    main()