import  socket

def main():
    #1.买个手机 创建socket
    tcp_server_socket=socket.socket(socket.AF_INET,socket.SOCK_STREAM)
    #2.插入电话卡，bind
    tcp_server_socket.bind(("",9484))
    #3.开铃声 listen 监听套接字负责等待有新的客户端进行连接
    tcp_server_socket.listen(128)
    print("already listened")
    while True:
        print("等待一个客户端的连接")
        #4.等待客户端链接,此时需要登台客户端的connect
        #new client是接下来给客户端访问的设备,会记录用户的socket信息
        new_client_socket,client_addr=tcp_server_socket.accept()

        print("一个新的客户端已经到来%s"%str(client_addr))
        #循环为一个客户端提供多次服务
        while True:
            recv_data=new_client_socket.recv(1024)
            print("客户端送过来的请求是:%s" % recv_data.decode(("utf-8")))
            #如果recv_data解堵塞，第一种客户端发送过来数据，第二种客户端close(发送过来空数据)
            if not recv_data :break
            else:
                new_client_socket.send("i get your msg".encode("utf-8"))
        #关闭和该客户端的连接
        #此时需要理解的是，当我在给a客户端服务器服务的时候，b服务器可以连接，但是会排队等待服务器空闲
        new_client_socket.close()
        print("已服务完毕")
    #监听套接字关闭 即相当于关闭服务器
    tcp_server_socket.close()

if __name__=="__main__":
    main()