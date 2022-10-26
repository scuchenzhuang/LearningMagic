import socket

def main():
    # 1.创建socket
    tcp_socket=socket.socket(socket.AF_INET,socket.SOCK_STREAM)

    # 2.链接服务器
    tcp_socket.connect(("10.211.55.4",50666))
    #server_ip=input("请输入要连接的服务器的ip")
    #server_port = int(input("请输入要连接的服务器的ip"))
    #tcp_socket.connect((server_ip,server_port))

    #3.发送数据
    send_data=input("请输入要发送的数据")
    tcp_socket.send(send_data.encode("utf-8"))

    #4.关闭
    tcp_socket.close()

if __name__=="__main__":
    main()