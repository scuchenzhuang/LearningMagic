from flask import Flask,render_template,request

app = Flask(__name__)

@app.route("/add/user",methods=['post','get'])
def add_user():
    if request.method == 'GET':
        return render_template("add_user.html")
    username = request.form.get('user')
    password = request.form.get('pwd')
    mobile = request.form.get('mobile')
    #1.连接mysql

    #2.执行sql
    #3.关闭连接

    return '添加成功'
@app.route('/show/user')
def show_user():
    print('展示所有的东西')
    data_list = [{'x':1},{'x':2},{'x':'1243'}]
    return render_template('show_user.html',data_list=data_list)

if __name__ == '__main__':
    app.run()