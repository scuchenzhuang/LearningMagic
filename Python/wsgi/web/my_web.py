import time

def application(environ, start_response):
    status = '200 OK'
    response_headers = [('Content-Type', 'text/html')]
    #元组里可以添加各种属性，

    start_response(status, response_headers)
    return str(environ) + '==Hello world from a simple WSGI application!--->%s\n' % time.ctime()