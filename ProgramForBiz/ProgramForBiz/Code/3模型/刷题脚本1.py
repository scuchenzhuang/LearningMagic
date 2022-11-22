from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
import time
import random
import threading


class Wenjuan(object):
    def __init__(self):
        option = webdriver.ChromeOptions()
        option.add_experimental_option('excludeSwitches', ['enable-automation'])
        option.add_argument('--disable-blink-features=AutomationControlled')
        option.add_argument('--headless')

        self.url = 'https://www.wjx.cn/vm/YjAaZuV.aspx'
        self.driver = webdriver.Chrome(options=option)

        # 调用回答问卷的函数
        self.answer_question()

    def answer_question(self):
        self.driver.get(self.url)
        self.driver.execute_script("var q=document.documentElement.scrollTop=0")
        time.sleep(2)
        WebDriverWait(self.driver,1000).until(
            EC.presence_of_element_located((By.ID, 'ctlNext'))
        )
        # 1、获取题目，遍历
        titles = self.driver.find_elements(By.XPATH, '//div[@class="field-label"]')
        for i in range(len(titles)):
            # 获取每个问题的
            try:
                answer1 = self.driver.find_elements(By.XPATH, f'//div[@id="div{i+1}"]//div[@class="ui-radio"]/div[@class="label"]')
                answer1[random.randint(0, len(answer1)-1)].click()
                time.sleep(0.2)
            except:
                try:
                    answer2 = self.driver.find_elements(By.XPATH, f'//div[@id="div{i+1}"]//div[@class="ui-checkbox"]')
                    li_num = []
                    for j in range(random.randint(3, len(answer2)-1)):
                        num = random.randint(0, len(answer2)-1)
                        if num not in li_num:
                            li_num.append(num)

                    for k in li_num:
                        answer2[k].click()
                        time.sleep(0.2)
                except:
                    continue
        # 点击提交
        self.driver.find_element(By.ID, 'ctlNext').click()
        time.sleep(0.5)

        try:
            comfirmdel = self.driver.find_element(By.XPATH, "/html/body/div[5]/div[2]/div[2]/button")
            self.driver.execute_script("arguments[0].click();", comfirmdel)
            time.sleep(0.5)
            comfirmdel = self.driver.find_element(By.XPATH, '//div[@id="rectTop"]')
            self.driver.execute_script("arguments[0].click();", comfirmdel)

        except:
            pass
        time.sleep(4)
        print(f'已填写完问卷')


'''
threads = []
page = int(input('请输入您要填写的问卷数目：'))
for i in range(page):  # 循环创建10个线程
    t = threading.Thread(target=Wenjuan)
    threads.append(t)

for t in threads:  # 循环启动10个线程
    t.start()
print(f'{page}份问卷自动填写完毕！')
'''
if __name__ == '__main__':
    Wenjuan()