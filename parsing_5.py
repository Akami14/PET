from selenium import webdriver
from selenium.webdriver.common.by import By
from time import sleep
from pymongo import MongoClient
import requests
import csv
from selenium.common.exceptions import NoSuchElementException

from selenium.webdriver.firefox.firefox_binary import FirefoxBinary
def urls_prepare(list_tikers, exchange = 'NASDAQ'):
    start_url = 'https://ru.tradingview.com/symbols/' + exchange + '-'
    end_url_marg = 'financials-income-statement/'
    urls = [start_url + ticker + '/' + end_url_marg for ticker in list_tikers]
    return urls




def parsing_tv(url):

    """ Функция собирает данные по списку url-ов и отправляет их в MongoDB """

    headers = {'User-Agent':
                   'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36'}
    url_ok = requests.get(url, headers=headers)
    sleep(0.5)

    if url_ok.ok:
        #binary = FirefoxBinary('path/to/binary')
        driver = webdriver.Firefox()
        #driver = webdriver.Chrome(r'chromedriver.exe')
        driver.get(url=url)
        driver.implicitly_wait(90)
        ticker_dict = {}
        try:
            total_revenue = driver.find_elements(By.XPATH, '//div[2]/div/div/div[5]/div[2]/div/div[1]/div[2]/div[5]/div/div/div[1]')
            total_revenue_list = []
            sleep(0.6)
            for el in total_revenue:
                a = el.text[1:-1]
                total_revenue_list.append(a)
                ticker_dict['url'] = url
                ticker_dict['total_revenue'] = total_revenue_list
            gross_profit = driver.find_elements(By.XPATH, '//div[2]/div/div/div[5]/div[2]/div/div[1]/div[4]/div[5]/div/div/div[1]')
            gross_profit_list = []
            for el in gross_profit:
                a = el.text[1:-1]
                gross_profit_list.append(a)
                ticker_dict['gross_profit'] = gross_profit_list
            net_income = driver.find_elements(By.XPATH, '//div[2]/div/div/div[5]/div[2]/div/div[1]/div[15]/div[5]/div/div/div[1]')
            net_income_list = []
            for el in net_income:
                a = el.text[1:-1]
                net_income_list.append(a)
                ticker_dict['net_income'] = net_income_list
        except NoSuchElementException:
            driver.close()

        p_2 = driver.find_element(By.XPATH, '//*[@id="earnings"]')
        p_2.click()
        sleep(1)
        try:
            eps_true = driver.find_elements(By.XPATH, '//div[2]/div/div/div[5]/div[2]/div/div[1]/div[2]/div[5]/div/div/div')
            eps_true_list = []
            for el in eps_true:
                a = el.text[1:-1]
                eps_true_list.append(a)
                ticker_dict['eps_true'] = eps_true_list[0:7]
            eps_predict = driver.find_elements(By.XPATH, '//div[2]/div/div/div[5]/div[2]/div/div[1]/div[3]/div[5]/div/div/div')
            eps_predict_list = []
            sleep(0.6)
            for el in eps_predict:
                a = el.text[1:-1]
                eps_predict_list.append(a)
                ticker_dict['eps_predict'] = eps_predict_list[0:7]
        except NoSuchElementException:
            driver.close()
        p_3 = driver.find_element(By.XPATH, '//div[4]/div[2]/div[2]/div/div/div[1]/div/div/a[3]')
        p_3.click()
        sleep(0.6)
        try:
            pe = driver.find_elements(By.XPATH, '//div[2]/div/div/div[6]/div[2]/div/div[1]/div[5]/div[5]/div/div/div')
            pe_list = []
            for el in pe:
                a = el.text[1:-1]
                pe_list.append(a)
                ticker_dict['pe'] = pe_list
            ebita = driver.find_elements(By.XPATH, '//div[2]/div/div/div[6]/div[2]/div/div[1]/div[10]/div[5]/div/div/div')
            ebita_list = []
            for el in ebita:
                a = el.text[1:-1]
                ebita_list.append(a)
                ticker_dict['ebita'] = ebita_list

        except NoSuchElementException:
            driver.close()

        driver.close()
        sleep(0.2)
        return ticker_dict
