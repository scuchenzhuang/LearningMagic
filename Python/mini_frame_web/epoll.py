#实现简单的http服务器
import re
import socket
import select
def service_client(new_socket):
    '''为这个客户端返回数据'''
    #接收浏览器发过来的请求
    # GET / HTTP/1.1

    request=new_socket.recv(1024).decode("utf-8")
    #print(">>>"*50)
    #print(request)
    #返回HTTP格式的数据，给浏览器
    request_lines=request.splitlines()
    #print("")
    #使用列表形式打开展示请求
    print(">"*20)
    print(request_lines)

    '''获取所需要的文件名'''
    #GET /index.html HTTP /1.1
    ret=re.match(r"[^/]+(/[^ ]*)",request_lines[0])
    if ret:
        file_name=ret.group(1)
        #print("*"*50,file_name)
        if file_name=="/":file_name="/CUHKSZ.html"


    #准备发送给浏览器的数据 header 在浏览器中用\r\n换行

    #body
    #response+="hahahahhaa"
    try:
        f=open("./html"+file_name,"rb")
    except:
        response="HTTP/1.1 404 NOT FOUND\r\n"
        response+="\r\n"
        response+="----------------File Not Found-------------"
        new_socket.send(response.encode("utf-8"))
    else:
        html_content=f.read()
        f.close()
        response = "HTTP/1.1 200 OK \r\n"
        response += "\r\n"
        # 发送header
        new_socket.send(response.encode("utf-8"))
        # 发送body
        new_socket.send(html_content)


    #关闭套接字
    new_socket.close()

def main():
    '''用来完成整体的控制'''
    #1.创建套接字
    tcp_server_socket=socket.socket(socket.AF_INET,socket.SOCK_STREAM)
    #2.绑定
    tcp_server_socket.bind(("",7892))
    #3.变为监听套接字
    tcp_server_socket.listen(128)
    tcp_server_socket.setblocking(False)#非阻塞
    #创建一个epoll对象
    epl=select.epoll()
    #将监听套接字对应的fd注册到epoll中,EPOLLING监测是否有输入，即客户端有链接的想法
    epl.register(tcp_server_socket.fileno(),select.EPOLLIN)
    fd_event_dict={}
    client_socket_list=list()
    while True:
        fd_event_list=epl.poll() #默认会堵塞直到os检测到数据到来，通过事件通知通知该程序，此时才是会接触堵塞
        #[(fd,event),即套接字对应的文件描述符，文件描述符是什么事件，例如可以recv接收]
        for fd,event in fd_event_list:
            # 4.可以收了，等待新客户端的连接
            #区别出监听
            if fd == tcp_server_socket.fileno():
                new_socket,client_addr=tcp_server_socket.accept()
                epl.register(new_socket.fileno(),select.EPOLLIN)
                fd_event_dict[new_socket.fileno()]=new_socket
            elif event==select.EPOLLIN:
                recv_data=fd_event_dict[fd].recv(1024).decode("utf-8")
                if recv_data:
                    service_client(fd_event_dict[fd],recv_data)
                else:
                    fd_event_dict[fd].close()
                    epl.unregister(fd)
                    del fd_event_dict[fd]
        #判断已经链接的客户端是否有数据发送过来


    #关闭监听套接字
    tcp_server_socket.close()





if __name__ == "__main__":
    main()