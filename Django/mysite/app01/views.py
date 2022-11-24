from django.shortcuts import render
from django.shortcuts import HttpResponse
# Create your views here.

def index(request):
    return HttpResponse("Welcome")

#命令行目录的项目，根据app的注册顺序逐一去寻找templates寻找
#对于pycharm创建的项目，会优先去项目的根目录去寻找
def user_list(request):
    return render(request,'user_list.html')
def tpl(request):
    name = '庄子'
    roles = ['job','age','gender']
    return render(
        request,
        'tpl.html',
        {
            'n1':name,
            'n2':roles
        }
    )

def news(request):
    import requests
    res = requests.get(url="https://www.chinaunicom.cn/api/article/NewsByIndex/2/2022/11/news",headers={"User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36"})
    data_list = res.json()
    print(data_list)
    return render(request,"news.html")

def feedback(request):
    pass
    #request是一个对象，封装了用户发送过来的所有请求相关的数据
    #request.method 其实就是获取请求方式
    #request.get 获取url传递的值
    #3.在请求体中提交数据，request.POST
    #4.httpresponse 内容字符串返回给请求者 HttpResponse("返回内容")
    #5.return render(html,dict)
    #6.响应
        # return redirect("www.baidu.com")重定向
        # 返回的是position,此时与原浏览器已经没有关系了
