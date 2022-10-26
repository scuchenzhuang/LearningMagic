import os
import  multiprocessing
def copy_file(file_name,old_folder_name,new_fold_name):
    """完成文件复制"""
    print("正在开始复制文件:%s,从%s--->到%s"%(file_name,old_folder_name,new_fold_name))
    old_f=open(old_folder_name+"/"+file_name,"rb")#rb二进制
    content=old_f.read()
    old_f.close()
    new_f=open(new_fold_name+"/"+file_name,"wb")
    new_f.write(content)
    new_f.close()

def main():
    #1.获取用户需要copy的文件夹
    #old_folder_name=input("请输入要copy的文件夹的名字")
    old_folder_name="/Users/chenzhuang/Desktop/CS/Linux"
    #2.创建文件夹
    try:
        new_folder_name=old_folder_name+"[复制]"
        os.mkdir(new_folder_name)
    except:
        #不存在文件就创建，否则就往后走
        pass

    #3.获取文件夹的所有的待copy的文件名字 listdir()
    file_names=os.listdir(old_folder_name)
    print(file_names)
    #4.创建进程池
    po = multiprocessing.Pool(5)
    #5.创建一个队列
    q=multiprocessing.Manager().Queue()
    #向进程池添加文件任务
    for file_name in file_names:
        po.apply_async(copy_file,args=(file_name,old_folder_name,new_folder_name))
    #复制原文件夹中的文件到新文件夹的文件去
    po.close()
    #po.join()
    while True:


if __name__ == "__main__":
    main()