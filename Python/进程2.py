import multiprocessing

def download_from_web(queue1,val):
    #模拟从网上下载了data
    data=[11,22,33,44]
    for temp in data:
        queue1.put(temp)
    print("已经下载好数据，并且存入队列中")

def analysis_data(queue1,val):
    waiting=[]
    while True:
        data=queue1.get()
        waiting.append(data)
        if queue1.empty():break

def main():
    #1创建一个队列
    queue1=multiprocessing.Queue()
    #2创建多个进程，把队列传参
    p1=multiprocessing.Process(target=download_from_web, args=(queue1,1))
    p2=multiprocessing.Process(target=analysis_data,args=(queue1,1))
    p1.start()
    p2.start()
if __name__ == "__main__":
    main()