import os
import openpyxl
import numpy as np
import pandas as pd
eng_to_month = {"Jan":'01',"Feb":'02',"Mar":'03',"Apr":'04',"May":'05',"Jun":'06',"Jul":'07',"Aug":'08',"Sep":'09',"Oct":'10',"Nov":'11',"Dec":'12'}
MA_parts = ['Area Planted','Area Harvested']
Bu_parts = ['Yield per Harvested Acre']


def dropna_list(nums):

    ans = []
    for num in nums:
        if num != 'nothing' and num:
            ans.append(num)

    return ans

def month_judge(month,tmplist):
    if month == '05':
        if len(tmplist) <= 3:tmplist.insert(2,'空')
        else:tmplist.insert(3,'空')
    return tmplist

def manipulate_df(df,year,month,category):

    ans_list = [[],[],[],[]]
    cur_month = month
    int_last_month = int(month) - 1
    if int_last_month <= 0:
        last_time = str(int(year)-1) +'12'
    else:
        if len(str(int_last_month)) >= 2:last_time = str(int(year)) + str(int_last_month)
        else:last_time = str(int(year)) +'0'+ str(int_last_month)
    cur_year = year#传入当前的年份
    cur_time = cur_year + cur_month
    times  = [str(int(year)-2)+cur_month,str(int(year)-1)+cur_month,last_time,cur_time]#时间序列
    for i,ans in enumerate(ans_list):
        ans_list[i].append(('category', category))
        ans_list[i].append(('Date',times[i]))
        ans_list[i].append(('ReleaseDate',cur_time))
    #Million Acres
    for inx in MA_parts:
        tmp_list = df.loc[inx]
        tmp_list.fillna('nothing', inplace=True)
        tmp_list = tmp_list.tolist()
        tmp_list = dropna_list(tmp_list)
        tmp_list = month_judge(month,tmp_list)
        for i in range(4):
            ans_list[i].append((inx,tmp_list[i], 'Millon Acres'))
    for inx in Bu_parts:
        tmp_list = df.loc[inx]
        tmp_list.fillna('nothing',inplace=True)
        tmp_list = tmp_list.tolist()
        tmp_list = dropna_list(tmp_list)
        tmp_list = month_judge(month, tmp_list)
        for i in range(4):
            ans_list[i].append((inx,tmp_list[i], 'Bushels'))
    MB_df = df.loc['Beginning Stocks':'Total']
    MB_df.reset_index(inplace=True)
    for t in range(len(MB_df)):
        tmp_list = MB_df.loc[t]
        tmp_list.fillna('nothing', inplace=True)
        tmp_list = tmp_list.tolist()
        tmp_list = dropna_list(tmp_list)
        tmp_list = month_judge(month, tmp_list)
        if len(tmp_list) < 5:pass
        else:
            for i in range(4):
                ans_list[i].append((tmp_list[0], tmp_list[i+1], 'Million Bushels'))
    return ans_list

def find_level(df,strx):
    index = df.index.values.tolist()
    for i,s in enumerate(index):
        if strx == s:
            return i
    return -1
'''生成年度'''
def make_year(reslist):
    for res in reslist:
        tar_date = res[1][1]
        year = tar_date[:4]
        month = tar_date[4] + tar_date[5]
        int_month = int(month)
        if int_month >= 5:
            period = year + '/' + str((int(year) + 1))
        else:
            period = str((int(year) - 1)) + '/' + year
        res.append(period)
    return reslist
#删除空属性的元素
def drop_res(reslist):
    ans = []
    for res in reslist:
        if res[3][1] == '空':
            pass
        else:
            ans.append(res)
    return ans

def write_excel(reslist):
    length = len(reslist[0])
    reslist = sorted(reslist,key=lambda x:x[-1])
    cur_period = None
    wb = openpyxl.load_workbook(filename='产出.xlsx')
    ws = wb.active
    for res in reslist:
        if not cur_period or cur_period != res[-1]:
            cur_period = res[-1]
            print(cur_period)
            ws.append([str(cur_period)])
        x_list = [res[2][1]]
        for i in range(3,16):
            x_list.append(res[i][1])
        ws.append(x_list)
    wb.save('产出.xlsx')





def main():
    whole_list = []
    for int_year in range(2010,2023):
        dir = './usda报告/'
        dir += str(int_year)
        dir += '/'
        file_names = os.listdir(dir)
        for file in file_names:
            month = file[-8] + file[-7]
            cur_dir = dir + file
            df =  pd.read_excel(cur_dir,sheet_name='Page 15',header=None)
            df.dropna(axis=0,how='all',inplace = True)
            df.dropna(axis=1, how='all', inplace=True)
            col1 = df.columns[0]
            df.set_index(col1,inplace=True)
            df = df.loc[:'SOYBEAN OIL']
            try:
                cur_list = manipulate_df(df,str(int_year),month,'SOYBEAN')
                for l in cur_list:
                    whole_list.append(l)
            except:
                print(str(int_year)+file+'出错')
    whole_list = make_year(whole_list)
    whole_list = drop_res(whole_list)
    write_excel(whole_list)


    #with open('输出.txt', 'a', encoding='utf-8') as ft:
    #    for tran in whole_list:
    #        ft.write(str(tran))
    #        ft.write('\n')






if __name__ == '__main__':
    main()