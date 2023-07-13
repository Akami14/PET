""" Преобрвзование полученных данных в более удобный вид, приведение
числовых данных к верному формату, разделение списков на отдельные столбцы датафрейма"""

import pandas as pd
from pymongo import MongoClient

def data_clear(base_path = 'trading', base_name = 'trading_2023_05', save_name='trading_data_cleared_2023_05.csv'):
    """Функция обрабатывает ранее собранные данные приводит их в числовой формат и сохраняет в csv"""

    client = MongoClient()
    trading = client[base_path]
    trading2 = trading[base_name]

    list_of_json = []
    for el in trading2.find():
        list_of_json.append(el)

    df = pd.DataFrame(list_of_json)
    df.drop(columns='_id', inplace=True)

    seria_temp = df['url'].copy()
    seria_temp.drop_duplicates(keep='first', inplace=True)
    list_of_true_index = list(seria_temp.index)
    df = df.loc[df.index[list_of_true_index]]

    col_for_drop = df.columns[1:]
    for col in df.columns[1:]:
        df[col] = [",".join(lists) for lists in df[col]]

    col_for_split_p1 = ['total_revenue', 'gross_profit', 'net_income', 'pe', 'ebita']
    for col in col_for_split_p1:
        list_of_name = [col + "_" + str(i) for i in range(1, 9)]
        df[list_of_name] = df[col].str.split(',', 8, expand=True)

    col_for_split_p2 = ['eps_predict', 'eps_true']
    for col in col_for_split_p2:
        list_of_name = [col + "_" + str(i) for i in range(1, 8)]
        df[list_of_name] = df[col].str.split(',', 7, expand=True)

    df.drop(columns=col_for_drop, inplace=True)

    return df



def data_to_numeric(df, save_name='trading_06_23.csv'):
    df = df.replace('', '0')
    for col_name in df.columns[1:]:
        for data in df[col_name]:
            if data != '':
                if data[-1] == 'M':
                    data_dict_replace1 = {}
                    if data[0] != '−':
                        value = float(data[0:-1]) * 1e6
                    else:
                        value = -float(data[1:-1]) * 1e6
                    data_dict_replace1[data] = str(value)
                    df.replace(data_dict_replace1, inplace=True)

    for col_name in df.columns[1:]:
        for data in df[col_name]:
            if data != '':
                if data[-1] == 'B':
                    data_dict_replace1 = {}
                    if data[0] != '−':
                        value = float(data[0:-1]) * 1e9
                    else:
                        value = -float(data[1:-1]) * 1e9
                    data_dict_replace1[data] = str(value)
                    df.replace(data_dict_replace1, inplace=True)

    for col_name in df.columns[1:]:
        for data in df[col_name]:
            if data != '':
                if data[-1] == 'K':
                    data_dict_replace1 = {}
                    if data[0] != '−':
                        value = float(data[0:-1]) * 1e3
                    else:
                        value = -float(data[1:-1]) * 1e3
                    data_dict_replace1[data] = str(value)
                    df.replace(data_dict_replace1, inplace=True)

    for col_name in df.columns[1:]:
        for data in df[col_name]:
            if data != '':
                data_dict_replace1 = {}
                if data[0] == '−':
                    value = -float(data[1:-1])
                    data_dict_replace1[data] = str(value)
                    df.replace(data_dict_replace1, inplace=True)

    for col in df.columns[1:]:
        df[col] = df[col].astype(float)

    df.to_csv(save_name, index=False)


df = data_clear()
data_to_numeric(df)
