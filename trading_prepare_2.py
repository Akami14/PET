import pandas as pd
import csv
""" Предподготовка данных, поиск тикеров Nasdaq и NYSE котрые есть в snp500 тикеры затем отправляются в csv файлы"""

wikipedia_data = pd.read_html('https://en.wikipedia.org/wiki/List_of_S%26P_500_companies')
snp_company_list = wikipedia_data[0] # the first table
snp_changes = wikipedia_data[1] # the second table

nasdaq = r'C:\Users\user\Desktop\DataScience\Selenium\nasdaq.csv'
nyse = r'C:\Users\user\Desktop\DataScience\Selenium\nyse.csv'

nasdaq_df = pd.read_csv(nasdaq)
nyse_df = pd.read_csv(nyse)

nasdaq_snp = [x for x in snp_company_list['Symbol'] for y in nasdaq_df['Symbol'] if x == y]
nyse_snp = [x for x in snp_company_list['Symbol'] for y in nyse_df['Ticker'] if x == y]

with open('nasdaq_snp.csv', 'w', newline='') as f:
    csv_writer = csv.writer(f)
    for item in nasdaq_snp:
        csv_writer.writerow([item])

with open('nyse_snp.csv', 'w', newline='') as f:
    csv_writer = csv.writer(f)
    for item in nyse_snp:
        csv_writer.writerow([item])
