import copy

import cx_Oracle
import pandas as pd
from collections import defaultdict
from datetime import datetime
date_map = {}
translation = {'Area Planted': '种植面积','Area Harvested':'收获面积','Yield':'单产','Beginning Stocks':'期初库存','Production':'总产量','Imports':'进口',' Supply, Total':'总供给','Crush':'压榨用量', 'Exports':'出口','Seed':'种子','Residual':'差值调整','Use, Total':'消费量','Ending Stocks':'期末库存'}
unit_map = {'Area Planted': '百万英亩','Area Harvested': '百万英亩','Yield':'蒲式耳','Beginning Stocks':'百万蒲式耳','Production':'百万蒲式耳','Imports':'百万蒲式耳',' Supply, Total':'百万蒲式耳','Crush':'百万蒲式耳','Exports':'百万蒲式耳','Seed':'百万蒲式耳','Residual':'百万蒲式耳','Use, Total':'百万蒲式耳','Ending Stocks':'百万蒲式耳'}
def read_excel(file = 'Soybeans_revise.csv'):
    df = pd.read_csv(file)
    df['年度'] = ['2008/2009'] * len(df)
    cur_year = '2008/2009'
    delete_line = []
    for i in range(len(df)):
        if len(df.iloc[i,0]) > 6:
            cur_year = df.iloc[i,0]
            delete_line.append(i)
        else:
            df.loc[i,'年度'] = cur_year
    df.drop(index=delete_line,inplace=True)
    return df

def make_info(df,freq='年度',varity='大豆'):
    db_writing = []

    group = df.groupby('Month')
    for k,tmp_df in group:
        for col in tmp_df.columns:
            if col in translation:

                vals = tmp_df.loc[:,col].tolist()
                written_map = defaultdict(str)
                written_map['val'] = vals[2]
                written_map['freq'] = freq
                written_map['varity'] = varity
                written_map['unit'] = unit_map[col]
                written_map['dataname'] = 'USDA：' + varity + '：'+translation[col] + '：' + str(vals[2]) + '：'+str(vals[1]) + '：'+str(vals[0])
                written_map['tablename'] = tmp_df.iloc[-1,-1]
                written_map['date'] = date_map[k]
                db_writing.append(copy.deepcopy(written_map))
    return db_writing


def create_date_map():
    with open('usda_report_date.txt') as f:
        while time:=f.readline():
            time = time.replace(' ','')
            time = time.replace('\n', '')
            _date = datetime.strptime(time, "%b%d,%Y")
            end_time = _date.strftime("%Y%m%d")
            if end_time[:6] not in date_map:
                date_map[end_time[:6]] = end_time

def into_db(maps):
    '''连接数据库'''
    tns = cx_Oracle.makedsn('10.243.73.69', 1521, 'orcl')
    db = cx_Oracle.connect('QIHUO_CTP_APP', 'QIHUO_CTP_APP', tns)
    for item in maps:
        str_getid = '''select max(id) from T_SCSJ_MYAGRIC_DATA'''
        cursor = db.cursor()
        cursor.execute(str_getid)
        new_id = int(cursor.fetchone()[0])
        new_id += 1
        str_diff = ''' INSERT INTO T_SCSJ_MYAGRIC_DATA VALUES(''' +str(new_id)+',' + str(item['date']) + ',' + str(item['dataname'])+ ',' + """null""" + ',' + str(item['unit']) + ',' + str(item['freq'])+ ',' + str(item['varity'])+ ',' +str(item['tablename'])+ ',' +"""null"""+ ','  +"""null"""+ ',' + str(item['val']) + ')'
        print(str_diff)


    
def main():
    create_date_map()
    df = read_excel()
    into_db(make_info(df))

if __name__ == '__main__':
    main()