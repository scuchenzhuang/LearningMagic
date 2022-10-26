import threading
import time

class MyThread(threading.Thread):#此时表示MyThread继承于父类threading.Thread
    def run(self):
        for i in range(3):
            time.sleep(1)
            print("I'm"+self.name+"@"+str(i))

if __name__  == "__main__":
    t=MyThread()
    t.start() #run方法重写了，调用start会自动执行run方法