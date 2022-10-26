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
def main():
    '''此处修改今日日期，与之比较的上周数据'''
    '''今日'''
    end_date = "20221021"
    '''与上周的哪一日进行比较'''
    last_week = '20221014'
    path = os.getcwd()
    path += "/" + end_date

    if not os.path.exists(path):
        os.makedirs(path)
        os.chmod(path, stat.S_IWOTH)


    jcl.main(_end_date=end_date,_last_week=last_week)
    kc.main(_end_date=end_date, _last_week=last_week)
    m_5d.main(_end_date=end_date, _last_week=last_week)
    m_20d.main(_end_date=end_date, _last_week=last_week)
    vol.main(_end_date=end_date, _last_week=last_week)
    mob.main(_end_date=end_date, _last_week=last_week)
    priceGraph.main(_end_date=end_date, _last_week=last_week)
    rol.main(_end_date=end_date, _last_week=last_week)
    profitGraph.main(_end_date=end_date, _last_week=last_week)
    rlt.main(_end_date=end_date,_last_week=last_week)



if __name__ == '__main__':
    main()