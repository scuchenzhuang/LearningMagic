import  socket

def send_file_to_client(new_client_socket):
    file_name = new_client_socket.recv(1024).decode("utf-8")
    print("客户端请求的文件名是:%s" % file_name)
    #用with打开的文件的前提是文件一定能打开
    file_content=None
    try:
        f=open(file_name,"rb")
        file_content=f.read()
        f.close()
    except Exception as ret:
        print("没有下载的文件%s" % file_name)
    if file_content:new_client_socket.send(file_content)

def main():
    #1.买个手机 创建socket
    tcp_server_socket=socket.socket(socket.AF_INET,socket.SOCK_STREAM)
    #2.插入电话卡，bind
    tcp_server_socket.bind(("",50322))
    #3.开铃声 listen 监听套接字负责等待有新的客户端进行连接
    tcp_server_socket.listen(128)
    print("already listened")
    while True:

        new_client_socket,client_addr=tcp_server_socket.accept()
        send_file_to_client(new_client_socket)
        new_client_socket.close()
    #监听套接字关闭 即相当于关闭服务器
    tcp_server_socket.close()

if __name__=="__main__":
    main()