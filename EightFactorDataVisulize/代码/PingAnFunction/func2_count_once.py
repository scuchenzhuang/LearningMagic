import pandas as pd
'''此函数判断last_week数据里到底是一个一维数组的data series还是有重复的dataframe'''
def count_once(dataframe_line):
    if isinstance(dataframe_line,pd.DataFrame):
        return dataframe_line.iloc[-1]
    else:
        return dataframe_line
