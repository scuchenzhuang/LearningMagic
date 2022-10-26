import threading
import time
import multiprocessing

def pre1():
    while True:
        print("1----------")
        time.sleep(1)

def pre2():
    while True:
        print("2----------")
        time.sleep(1)

def main():
    # t1=threading.Thread(target=pre1)
    # t2=threading.Thread(target=pre2)
    # t1.start()
    # t2.start()
    t1=multiprocessing.Process(target=pre1)
    t2=multiprocessing.Process(target=pre2)
    t1.start()
    t2.start()

if __name__ == "__main__":
    main()