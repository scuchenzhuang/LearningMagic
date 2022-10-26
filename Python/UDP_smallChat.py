import socket

def send_msg(udp_socket):
    print("开始输入操作")
    dest_ip=input("请输入对方的ip:")
    dest_port=int(input("请输入对方的port:"))
    send_data=input("请输入要发送的信息")
    udp_socket.sendto(send_data.encode("utf-8"),(dest_ip,dest_port))

def recv_msg(udp_socket):
    recv_data=udp_socket.recvfrom(1024)
    print(recv_data)

def main():
    udp_socket=socket.socket(socket.AF_INET,socket.SOCK_DGRAM)
    #udp_socket.bind(("10.211.55.2",50888))
    print("socket已经创建 固定端口50888")
    while True:
        print("-------庄庄聊天器-------")
        print("1：发送信息")
        print("2：接收信息")
        print("3：退出")
        op=int(input("请选择功能"))
        #发送
        if op==1:send_msg(udp_socket)
        #接收并显示
        elif op==2:recv_msg(udp_socket)
        elif op==3:break
        else:print("输入有误")

if __name__=="__main__":
    main()