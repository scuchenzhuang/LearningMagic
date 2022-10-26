import socket

def main():
    #创建一个UDP套接字
    udp_socket=socket.socket(socket.AF_INET,socket.SOCK_DGRAM)
    #可以使用套接字收发数据
    #关闭套接字
    #
    for i in range(5):
        tmp="Hello I will use for Loop to bang you~~~yeah this is the"+str(i)+"time"
        tmp=tmp.encode('utf-8')
        udp_socket.sendto(tmp,("10.211.55.4",8080))
    print("Socket is running")
    #将某个套接字socket设置一个固定的端口号用于收数据
    #udp_socket.bind(local_addr)
    udp_socket.bind(("10.211.55.2",24542))
    recv_data=udp_socket.recvfrom(1024)
    recv_msg=recv_data[0]
    send_addr=recv_data[1]
    print(str(recv_msg))
    print(str(send_addr))
    udp_socket.close()
if __name__=="__main__":
    main()
