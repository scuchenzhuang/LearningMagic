import os
import random          # 用于产生随机数
import time            # 用于延时
from selenium.webdriver.common.by import By      #导入By包进行元素定位
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from multiprocessing import Process
import threading
# 实例化一个启动参数对象
chrome_options = Options()

# 添加启动参数
chrome_options.add_argument(
    'user-agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.130 Safari/537.36"')  # 添加请求头
chrome_options.add_argument('--disable-blink-features=AutomationControlled')

# 防止被识别
chrome_options.add_experimental_option('excludeSwitches', ['enable-automation'])  # 设置开发者模式启动

chrome_options.add_experimental_option('useAutomationExtension', False)  # 关闭selenium对chrome driver的自动控制

#chrome_options.maximize_window()  # 网页最大化

# chrome_options.add_argument('headless')    #设置浏览器以无界面方式运行
browser = webdriver.Chrome(options=chrome_options)     #设置驱动程序，启动浏览器  （实现以特定参数启动）
browser.execute_cdp_cmd('Page.addScriptToEvaluateOnNewDocument',
                        {'source': 'Object.defineProperty(navigator, "webdriver", {get: () => undefined})'})       #用来执行Chrome开发这个工具命令
count = 0

def run():
    option = webdriver.ChromeOptions()
    option.add_experimental_option('excludeSwitches', ['enable-automation'])
    option.add_experimental_option('useAutomationExtension', False)
    browser = webdriver.Chrome(options=option)
    browser.execute_cdp_cmd('Page.addScriptToEvaluateOnNewDocument',
                           {'source': 'Object.defineProperty(navigator, "webdriver", {get: () => undefined})'})
    browser.get("https://www.wjx.cn/vm/YjAaZuV.aspx")

    '''填空题 第一题'''
    # 自定义要填的内容
    age = random.randint(20,40)
    # 在题目中随机输入上述内容
    browser.find_element(By.ID,'q1').send_keys(age)
    #browser.find_element_by_id("q1").send_keys(age)
    # 延时
    time.sleep(0.3)

    for tran in range(2,16):
        '''选择题'''
        randomId = random.randint(1, 2)  # 随机点击第一个选项或第二个选项
        # js实现方式

        js = "document.getElementById(\"q" + str(tran) + "_" + str(randomId) + "\").checked = true"
        browser.execute_script(js)  # 使用js实现点击的效果（调用js方法，同时执行javascript脚本）
        js = "document.getElementById(\"q" +str(tran) + "_"+ str(randomId) + "\").click()"
        browser.execute_script(js)  # 使用js实现点击的效果（调用js方法，同时执行javascript脚本）

        # 延时 太快会被检测是脚本
        time.sleep(0.1)
    submit_button = browser.find_element(By.ID,'divSubmit')  # 找到提交按钮
    submit_button.click()  # 点击提交
    # 智能验证
    '''
    while 'mainBgColor' in browser.page_source:
        mainBgColor = browser.find_element(By.CLASS_NAME,'mainBgColor')
        mainBgColor.click()
    '''
    # 模拟点击智能验证按钮
    # 先点确认
    '''
    browser.find_element(By.XPATH,"//button[text()='确认']").click()
    time.sleep(1)
    # 再点智能验证提示框，进行智能验证
    browser.find_element_by_xpath("//div[@id='captcha']").click()
    '''


    try:

        comfirmdel = browser.find_element(By.XPATH, "/html/body/div[7]/div[3]/a[1]")
        browser.execute_script("arguments[0].click();", comfirmdel)
        time.sleep(0.5)
        comfirmdel = browser.find_element(By.XPATH, '/html/body/div[1]/form/div[6]/div[9]/div[2]/div/div/div/div[1]/div[2]/div[1]')
        browser.execute_script("arguments[0].click();", comfirmdel)
    except:
        print('出错')
    time.sleep(10)
    global count
    count += 1
    print("已填写"+str(count)+'个问卷')

if __name__ == '__main__':
    threads = []

    count = 0
    '''
    
    for i in range(10):  # 循环创建10个线程
        t = threading.Thread(target=run)
        threads.append(t)

    for t in threads:  # 循环启动10个线程
        t.start()
    '''
    while True:
        run()


