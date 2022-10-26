import time
import threading

def pretest1():
    for i in range(5):print("test1第%d次" % i)
    #如果创建Thread时执行的函数结束了，运行结束意味着，这个子线程结束了

def pretest2():
    for i in range(5):print("test2第%d次" % i)

def saySorry(NameJin):     #子线程
    print("亲爱的"+NameJin+"我错了，能吃饭吗")
    time.sleep(1)

def main():
    t1=threading.Thread(target=pretest1)
    t2 = threading.Thread(target=pretest2)
    t3=threading.Thread(target=saySorry,args=("金鱼",))#多线程是元组传递，这个，不能省略
    print("此时线程既没创建线程，也没执行线程"+str(threading.enumerate()))
    t1.start()#调用start时候才会执行此线程
    time.sleep(1)
    print("t1执行完了,主线程睡醒了")
    t2.start()
    time.sleep(1)
    print("t2执行完了,主线程睡醒了")
    t3.start()
    print(threading.enumerate())
    #主线程结束以后，整个程序才能结束
if __name__ == "__main__":
    main()