import socket
def main():
    tcp_client_file=socket.socket(socket.AF_INET,socket.SOCK_STREAM)
    #连接服务器
    tcp_client_file.connect(("10.211.55.4",7999))
    file_name=input("要下载的文件名")
    #发送下载文件的请求
    tcp_client_file.send(file_name.encode("utf-8"))
    #接收对方发过来的数据，最多1k
    recv_data=tcp_client_file.recv(1024)
    if recv_data:
        #如果在write操作抛出错误，那么会关闭文件。不会循环，只会使用一次
        with open("接收"+file_name,"wb") as f:
            f.write(recv_data)
    tcp_client_file.close()

if __name__=="__main__":
    main()