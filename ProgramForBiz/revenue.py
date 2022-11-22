import math
import statsmodels.api as sm
from sklearn.linear_model import LinearRegression
import numpy as np
import pandas as pd
from sklearn.model_selection import train_test_split
def main():
    df = pd.read_csv('target.csv')

    df.drop(columns='ID',inplace=True)
    df.dropna(how='all',axis=1,inplace=True)
    for i in range(len(df)):
        tmp = df.loc[i,'1st Markdown 幅度%']
        df.loc[i,'1st Markdown 幅度%'] = '0.' + tmp[:-1]
        #df.loc[i, '1st Markdown 幅度%'] =  tmp[:-1]

    for i in range(len(df)):
        tmp = df.loc[i,'1st Markdown 时间%']
        df.loc[i,'1st Markdown 时间%'] = '0.' + tmp[:-1]
        #df.loc[i,'1st Markdown 时间%'] =  tmp[:-1]

    #log下什么都不写默认是自然对数
    df['1st Markdown 幅度%']=df['1st Markdown 幅度%'].astype(float)
    df['1st Markdown 时间%']=df['1st Markdown 时间%'].astype(float)

    df['1st Markdown 幅度%'] = np.log(df['1st Markdown 幅度%'])
    df['1st Markdown 时间%'] = np.log(df['1st Markdown 时间%'])
    X = df
    y = np.log(df['Per Revenue'])

    alg = LinearRegression()
    # 拟合

    alg.fit(df[['1st Markdown 幅度%','1st Markdown 时间%'] ], y)

    # 查看回归系数
    print('回归系数：', alg.coef_)  # 返回的是一个数组
    # 查看截距
    print('截距：', alg.intercept_)

    #statamodel
    # 多元线性拟合回归
    x_n = sm.add_constant(df[['1st Markdown 幅度%','1st Markdown 时间%'] ])
    # 线性回归拟合
    model_stata = sm.OLS(y, x_n)  # model是回归分析模型
    results = model_stata.fit()  # results是回归分析后的结果

    # 输出回归分析的结果
    print(results.summary())
    print('Parameters: ', results.params)
    print('R2: ', results.rsquared)


    '''预测训练部分'''
    df['Per Revenue'] = df['Per Revenue'].astype(int)
    y = df['Per Revenue']
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.3, random_state=1234)
    from sklearn.linear_model import LogisticRegression
    model = LogisticRegression()
    model.fit(X_train,y_train)
    y_pred = model.predict(X_test)

    print(model.coef_)


if __name__  == '__main__':
    main()