import pandas as pd
import numpy as np
import re
import os
import ipywidgets as widgets
from ipywidgets import HBox, VBox
import io
import datetime as dt
from pandas import ExcelWriter

def PK_FUCKtura(df_blocks, df_calls):
    #Формируем датафрейм со всеми L-блокировками
    df_blocks['ОСНОВНОЙ НОМЕР'] = df_blocks['ОСНОВНОЙ НОМЕР'].str[-10:]
    df_blocks['ДОПОЛНИТЕЛЬНЫЕ НОМЕРА'] = df_blocks['ДОПОЛНИТЕЛЬНЫЕ НОМЕРА'].str[-10:]
    df_blocks = df_blocks.rename(columns = {'ОСНОВНОЙ НОМЕР' : 'Телефон', 'ДОПОЛНИТЕЛЬНЫЕ НОМЕРА' : 'Дополнительный телефон', 'ФИО СОТРУДНИКА, совершившего первичную обработку' : 'Оператор'})
    #если поле завершения обработки пустое, то датой завершения обработки считаем следующий день
    df_blocks['ВРЕМЯ ЗАВЕРШЕНИЯ ОБРАБОТКИ'] = df_blocks['ВРЕМЯ ЗАВЕРШЕНИЯ ОБРАБОТКИ'].fillna(df_blocks['ВРЕМЯ ПЕРВИЧНОЙ ОБРАБОТКИ'].dt.date + dt.timedelta(days = 1))
    df_blocks_FZ = df_blocks[df_blocks['ИСХОДНЫЙ ФРОД-СТАТУС'] == 'L']
    
    #Формируем датафрейм со всеми звонками по Фактуре
    df_calls = df_calls[df_calls['Unnamed: 0'].str[:2] != '48']
    df_calls = df_calls[:-1]
    df_calls.columns = ['Телефон', 'Оператор', 'Начало звонка', 'Набор номера', 'Неудачная попытка', 'Отменен']
    df_calls['Оператор'] = df_calls['Оператор'].str[4:]
    df_calls['Телефон'] = df_calls['Телефон'].str[-10:]

    #Потом убрать!
    df_blocks_FZ['ВРЕМЯ ПЕРВИЧНОЙ ОБРАБОТКИ'] = df_blocks_FZ['ВРЕМЯ ПЕРВИЧНОЙ ОБРАБОТКИ'] + dt.timedelta(minutes = 30)
    
    #Мерджим ФЗ-блокировки и звонки, 
    #получаем датафрейм с "хорошими звонками": звонок совершен, время звонка между блокировкой и первичной обработкой,
    #находим операции, по которым не было звонка (не соответсвуют условиям выше)
    first_calls = df_blocks_FZ.merge(df_calls, on = ['Телефон', 'Оператор'])
    first_calls = first_calls.loc[(first_calls['Начало звонка']>=first_calls['ВРЕМЯ БЛОКИРОВКИ'])
                                  &(first_calls['Начало звонка']<=first_calls['ВРЕМЯ ПЕРВИЧНОЙ ОБРАБОТКИ'])]

    #Проверяем, что по этому номеру телефона нет звонка
    bad_calls = df_blocks_FZ[~df_blocks_FZ['НОМЕР ОПЕРАЦИИ'].isin(first_calls['НОМЕР ОПЕРАЦИИ'])]

    #Выделяем блокировки, по которым должен быть повторный звонок
    df_blocks_2calls = df_blocks[df_blocks['Первичный комментарий'].str[:11] == 'Перезвонить']
    #Вставляем новый столбец со временем перезвона, часы и минуты
    df_blocks_2calls.insert(12, 'Время второго звонка', df_blocks_2calls['Первичный комментарий'].str[14:19].str.split(':'))

    #Мерджим со звонками
    second_calls = df_blocks_2calls.merge(df_calls, on = 'Телефон')
    #Оставляем звонки, где начало звонка +-10 минут от заявленного в комментарии времени 
    second_calls = second_calls.loc[abs((second_calls['Начало звонка'].dt.hour-second_calls['Время второго звонка'].str[0].astype(int))*60
                                        +(second_calls['Начало звонка'].dt.minute-second_calls['Время второго звонка'].str[1].astype(int)))<=10]
    #и время звонка между первичной обработкой и завершением обработки
    second_calls = second_calls.loc[(second_calls['Начало звонка']>=second_calls['ВРЕМЯ ПЕРВИЧНОЙ ОБРАБОТКИ'])
                                        &(second_calls['Начало звонка']<=second_calls['ВРЕМЯ ЗАВЕРШЕНИЯ ОБРАБОТКИ'])]
    
    #Выводим операции, которые не подпали под эти критерии
    bad_second_calls = df_blocks_2calls[~df_blocks_2calls['НОМЕР ОПЕРАЦИИ'].isin(second_calls['НОМЕР ОПЕРАЦИИ'])]
    
    #Формируем датафрейм с блокировками, по которым не выполнен SLA
    bad_sla = df_blocks.loc[(df_blocks['ВРЕМЯ ПЕРВИЧНОЙ ОБРАБОТКИ'] - df_blocks['ВРЕМЯ БЛОКИРОВКИ']) > dt.timedelta(minutes = 30)]

    #Недозвоны, где звонок отменен по инициативе оператора
    failed_calls = df_calls[(df_calls['Неудачная попытка'] == 1)&(df_calls['Отменен'] == 1)]
    
    with ExcelWriter('result.xlsx', engine="openpyxl", mode='w') as writer:
        bad_calls.to_excel(writer, sheet_name="bad_calls_FZ", index = False)

    with ExcelWriter('result.xlsx', engine="openpyxl", mode='a') as writer:
        bad_second_calls.to_excel(writer, sheet_name="bad_second_calls", index = False)

    with ExcelWriter('result.xlsx', engine="openpyxl", mode='a') as writer:
        bad_sla.to_excel(writer, sheet_name="bad_sla", index = False)
                
    with ExcelWriter('result.xlsx', engine="openpyxl", mode='a') as writer:
        failed_calls.to_excel(writer, sheet_name="failed_calls", index = False)

    return [bad_calls, bad_second_calls, bad_sla, failed_calls]

def Start():
    uploader1 = widgets.FileUpload(multiple=True)
    print('Выберите файл(-ы) с отчетами по Фактуре:')
    display(uploader1)
    uploader2 = widgets.FileUpload(multiple=True)
    print('Выберите файл с отчетом по звонкам:')
    display(uploader2)
    
    button = widgets.Button(description='Запустить ПК')
    out = widgets.Output()
    def on_button_clicked(_): #кнопка для загрузки файлов и запуска функции PK_FUCKtura
        # "linking function with output"
        with out:
            # what happens when we press the button
            out.clear_output()
            uploaded_file1 = uploader1.value
            df_blocks = pd.read_excel(io.BytesIO(uploaded_file1[0].content), dtype = ({'ОСНОВНОЙ НОМЕР' : str, 'ДОПОЛНИТЕЛЬНЫЕ НОМЕРА' : str}))
            for i in range(1,len(uploaded_file1),1):
                df_blocks = pd.concat([df_blocks, pd.read_excel(io.BytesIO(uploaded_file1[0].content), dtype = ({'ОСНОВНОЙ НОМЕР' : str, 'ДОПОЛНИТЕЛЬНЫЕ НОМЕРА' : str}), usecols = [0,4,5,6,7,8,9,10,11,12,13,14,15])])

            uploaded_file2 = uploader2.value[0]
            df_calls = pd.read_excel(io.BytesIO(uploaded_file2.content), header = 2, usecols = [0, 1, 2, 6, 16, 22], dtype = ({'Unnamed: 0' : str}))
            for i in range(1,len(uploaded_file2),1):
                df_calls = pd.concat([df_calls, pd.read_excel(io.BytesIO(uploaded_file2.content), header = 2, usecols = [0, 1, 2, 6, 16, 22], dtype = ({'Unnamed: 0' : str}))])
            result = PK_FUCKtura(df_blocks, df_calls)

            if result[0].empty and result[1].empty and result[2].empty and result[3].empty:
                print('Ошибки отсутствуют')
            if result[0].empty == False:
                print('\nОтсутствуют звонки по ФЗ-блокировкам:')
                display(result[0].iloc[:,[0,4,6,7,8,11,13]].reset_index(drop=True))
            if result[1].empty == False:
                print('\nОтсутствуют звонки по блокировкам, по которым требовался повторный звонок:')
                display(result[1].iloc[:,[0,4,6,7,8,10,13,14,15]].reset_index(drop=True))
            if result[2].empty == False:
                print('\nНе выполнен SLA по времени обработки блокировок:')
                display(result[2].reset_index(drop=True))
            if result[3].empty == False:
                print('\nЗвонки отменены до автоматического разъединения:')
                display(result[3].reset_index(drop=True))                      
    # linking button and function together using a button's method
    button.on_click(on_button_clicked)
    print('После выбора файлов нажмите:')
    display(button)
    
    print('Результат ПК отобразится ниже после нажатия кнопки, а также будет сохранен в файле result.xlsx')
    display(out)
