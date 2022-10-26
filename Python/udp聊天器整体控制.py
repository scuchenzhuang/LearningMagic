import  socket
import threading
def send_msg(udp_socket,ip):
    while True:
        send_data=input("输入你要发送的内容")
        udp_socket.sendto(send_data.encode("utf-8"),(ip,8919))

def recv_msg(udp_socket):
    while True:
        recv_data=udp_socket.recvfrom(1024)
        print(recv_data)


def main():
    """完成udp聊天器的整体控制"""
    #创建socket
    udp_socket=socket.socket(socket.AF_INET,socket.SOCK_DGRAM)
    #绑定端口
    udp_socket.bind(("",7890))
    t_recv=threading.Thread(target=recv_msg , args=(udp_socket,))
    t_send=threading.Thread(target=send_msg , args=(udp_socket,"10.211.55.4"))
    t_recv.start()
    t_send.start()

if __name__ == "__main__":
    main()