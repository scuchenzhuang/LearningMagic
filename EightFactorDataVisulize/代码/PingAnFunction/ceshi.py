import cx_Oracle
import pandas as pd
'''连接数据库'''
tns = cx_Oracle.makedsn('10.243.73.69', 1521, 'orcl')
db = cx_Oracle.connect('QIHUO_CTP_APP', 'QIHUO_CTP_APP', tns)

def main():
    df = pd.read_excel("../../配置/配置.xlsx", sheet_name="基差")
    diff_map = {}  # 基差查询映射
    for i in range(len(df)):
        tuple_x = df.iloc[i, :]  # 取第一行的数据 以tuple的形式
        tmp_name = tuple_x['f_name']  # 期货物品名字
        tmp_dataname = tuple_x['data_name']
        tmp_table = tuple_x['db_table']
        diff_map[tmp_name] = (tmp_dataname,tmp_table)
    ans_list = []
    for cur in diff_map:
        tmp_dataname = diff_map[cur][0]
        tmp_table = diff_map[cur][1]
        str_diff = ''' select * from ''' + tmp_table + ''' where F_DATANAME =''' + """'""" + tmp_dataname + """'""" + ''' and F_DATE='20221020' '''
        cursor = db.cursor()
        cursor.execute(str_diff)
        tmp_list = list(cursor.fetchall())
        if len(tmp_list) >= 2:print(tmp_list)

if __name__ == '__main__':
    main()