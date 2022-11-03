#实现简单的http服务器
import re
import socket
import multiprocessing
import time


class WSGIServer(object):
    def __init__(self):
        '''用来完成整体的控制'''
        # 1.创建套接字
        self.tcp_server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        '''tcp4次挥手，如果是服务器/用户先提出挥手 那么服务器/用户会首先等待2~4分钟才能释放端口使用资源'''
        '''而加上这句话以后 不需要等待'''
        self.tcp_server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        # 2.绑定
        self.tcp_server_socket.bind(("", 7890))
        # 3.变为监听套接字
        self.tcp_server_socket.listen(128)

    def service_client(self,new_socket):
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
        file_name = ""
        #GET /index.html HTTP /1.1
        ret=re.match(r"[^/]+(/[^ ]*)",request_lines[0])
        if ret:
            file_name=ret.group(1)
            #print("*"*50,file_name)
            if file_name=="/":file_name="/index.html"


        #准备发送给浏览器的数据 header 在浏览器中用\r\n换行

        #body
        #response+="hahahahhaa"
        if not file_name.endswith(".py"):
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
        else:
            '''如果以.py结尾那么视为动态资源的请求'''
            header = "HTTP/1.1 200 OK \r\n"
            header += "\r\n"
            body = "this is part of my life %s " % time.ctime()
            response = header + body
            '''发送response给浏览器'''
            new_socket.send(response.encode("utf-8"))


        #关闭套接字
        new_socket.close()

    def run_forever(self):
        while True:
        #4.等待新客户端的连接
            new_socket,client_addr=self.tcp_server_socket.accept()
            p=multiprocessing.Process(target=self.service_client,args=(new_socket,))
            p.start()
            new_socket.close()
        #5.为这个客户端服务
        #关闭监听套接字
        self.tcp_server_socket.close()




def main():
    #控制整体，创建web服务对象 调用forever方法运行
    wsgi_server = WSGIServer()
    wsgi_server.run_forever()

if __name__ == "__main__":
    main()