import os
import openpyxl
import pandas as pd
import numpy as np

SoyMapKeys = [ 'Area Planted' ,'Area Harvested', 'Yield per Harvested Acre', 'Beginning Stocks', 'Production', 'Imports', '    Supply, Total', 'Crushings', 'Exports', 'Seed', 'Residual', '    Use, Total', 'Ending Stocks', 'Avg. Farm Price ($/bu)  2/']
SoyOilKeys = []
key_chosen = {'soybeans':SoyMapKeys}
def soybeans(year,month,df,category):
    reslist = []
    #去除掉倒数第二行，没有用
    soy_df = df.drop(labels=df.columns[-2],axis=1)
    '''之后按照一定要求排序，[种类，发布日期，年度，{'Seed':值...etc}]'''
    #获取年度
    int_month = int(month)
    if int_month >= 5:
        period_now = year + '/' + str((int(year) + 1))
        period_last = str((int(year) - 1)) + '/' + year
        period_last2 = str((int(year) - 2)) + '/' + str((int(year) - 1))
    else:
        period_now = str((int(year) - 1)) + '/' + year
        period_last = str((int(year) - 2)) + '/' + str((int(year) - 1))
        period_last2 = str((int(year) - 3)) + '/' + str((int(year) - 2))

    reslist.append([category, year + month, period_last2, {}])
    reslist.append([category, year + month, period_last, {}])
    reslist.append([category, year + month, period_now, {}])
    for i in range(len(soy_df)):
        for j in range(3):
            tmp_map = reslist[j][-1]
            tmp_map[soy_df.iloc[i,0]] = soy_df.iloc[i,j+1]

    return reslist


def write_excel(file,reslist,sheet_name,category):
    cur_period = None
    wb = openpyxl.load_workbook(filename=file)
    ws = wb[sheet_name]
    for res in reslist:
        if not cur_period or cur_period != res[2]:
            cur_period = res[2]
            ws.append([str(cur_period)])
        x_list = [res[1]]
        cur_map = res[3]
        for key in key_chosen[category]:
            if key not in cur_map:
                x_list.append('空')
            else:
                x_list.append(cur_map[key])
        ws.append(x_list)
    wb.save(file)
    print("finished")

def sort_res(reslist):
    reslist = sorted(reslist, key=lambda x: x[1])
    reslist = sorted(reslist, key=lambda x: x[2])
    return reslist


def read_file():
    SoybeanList = []
    SoyOilList = []
    SoyMealList = []
    dir = './usda报告'
    yearlist = os.listdir(dir)
    for year in yearlist:
        dir = './usda报告'
        dir += '/' + year
        filenames = os.listdir(dir)
        for file in filenames:
            cur_dir = dir + '/' + file
            month = file[-8] + file[-7]
            df = pd.read_excel(cur_dir,sheet_name='Page 15')
            df.dropna(axis=0, how='all', inplace=True)
            df.dropna(axis=1, how='all', inplace=True)
            df.reset_index(inplace=True)
            df.drop('index',axis=1,inplace=True)
            del_indexs = []
            for i in range(len(df)):
                if type(df.iloc[i,0]) != type('test') or df.iloc[i,0] == 'Filler':
                    del_indexs.append(i)
            df.drop(labels = del_indexs,axis = 0,inplace=True)
            df.dropna(axis=1,how='all',inplace=True)
            df.reset_index(drop=True,inplace=True)
            position_list = {}
            for i in range(len(df)):
                position_list[df.iloc[i,0]] = i

            SoybeanList.extend(soybeans(year,month,df[:position_list['SOYBEAN OIL']],'SOYBEANS'))
            SoyOilList.extend(soybeans(year,month,df[position_list['SOYBEAN OIL']:position_list['SOYBEAN MEAL']],'SOYBEAN OIL'))
            SoyMealList.extend(soybeans(year,month,df[position_list['SOYBEAN MEAL']:],'SOYBEAN MEAL'))
    SoybeanList = sort_res(SoybeanList)
    SoyOilList = sort_res(SoyOilList)
    SoyMealList = sort_res(SoyMealList)
    write_excel('Soybeans.xlsx',SoybeanList,'Soybeans','soybeans')


            #
            #


def main():
    read_file()



if __name__ == '__main__':
    main()