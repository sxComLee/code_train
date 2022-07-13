#!/usr/bin/env python
# -*-- coding: utf-8 -*--
# 秒杀脚本测试
from selenium import webdriver
import datetime
import time
from os import path
from selenium.webdriver.common.action_chains import ActionChains

d = path.dirname(__file__)
abspath = path.abspath(d)

driver = webdriver.Firefox()
driver.maximize_window()

def login() :
# 打开登陆页面，并进行扫码登陆
    driver.get("https://taobao.com")
    time.sleep(3)
    if driver.find_element_by_link_text("亲，请登录"):
        driver.find_element_by_link_text("亲，请登录").click()
        #扫码登录
        print("请在20s内扫码")
        time.sleep(20)
        #进入购物车
        driver.get("https://cart.taobao.com/cart.htm")
        time.sleep(3)

        now = datetime.datetime.now()
        print("login success:",now.strftime("%Y-%m-%d %H:%M:%S"))

#点击结算
def buy(buytime):
    while True :
        now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        #对比时间，时间到了的话就点击结算呢
        if now > buytime:
            while True:
                try:
                    print("开始全选")
                    # 点击购物车全选按钮
                    # if driver.find_element_by_id("J_CheckBox_1763262818054"):
                    #     driver.find_element_by_id("J_CheckBox_1763262818054").click()
                    # if driver.find_element_by_id("J_CheckShop_s_1761184661_1"):
                    #     driver.find_element_by_id("J_CheckShop_s_1761184661_1").click()
                    if driver.find_element_by_xpath('//*[@id="J_SelectAll1"]/div/label'):
                        driver.find_element_by_xpath('//*[@id="J_SelectAll1"]/div/label').click()
                        break
                except:
                    print("找不到全选按钮")
            while True:
                try:
                    print("开始结算")
                    #点击结算按钮
                    if driver.find_element_by_xpath('//*[@id="J_Go"]/span'):
                        driver.find_element_by_xpath('//*[@id="J_Go"]/span').click()
                        break
                except:
                    print("找不到结算按钮")
            while True:
                try:
                    print("开始提交订单")
                    # 点击提交订单
                    if driver.find_element_by_xpath('//*[@id="submitOrderPC_1"]/div/a[2]'):
                        driver.find_element_by_xpath('//*[@id="submitOrderPC_1"]/div/a[2]').click()
                        break
                except:
                    print("找不到结算按钮")

if __name__ == "__main__":
    # times = input('请输入抢购时间')
    #时间格式：'2020-01-01 11：20：00。000000'
    login()
    buy("2020-01-01 11:20:00")

