import pandas as pd
import numpy as np
import re
import os
import ipywidgets as widgets
from ipywidgets import HBox, VBox
import io

def PK_SMS(df_sms, df_crm):
    df_sms_clean = df_sms.drop(columns = 'Дата создания') 
    #создан df для последующего использования

    df_sms_clean['MaskPAN'] = df_sms_clean['Текст'].str.extract('(\d+)') 
    #методом .str.extract используя regex извлекаем ПАНы по шаблону \d+ 'одна или несколько цифр'

    df_sms_date_temp = df_sms_clean['Текст'].str.extract('(\d\d\.\d\d\.\d\d\d\d\s\d\d:\d\d:\d\d)|(\d\d\.\d\d\s\d\d:\d\d:\d\d)')
    #т.к. в сообщениях присутствует несколько форматов дат, используем два шаблона regex. Метод .str.extract сам создает новый временный df

    df_sms_date_temp.fillna(' ', inplace=True) #для возможности дальнейшего объединения строк нужно заполнить пустые строки
    df_sms_date_temp[1] = df_sms_date_temp[1].apply(lambda x: re.sub(r'(\d{2}\.\d{2})(\S*)', r'\1.2023', str(x))) 
    #приводим даты к единому виду, добавляя где надо год. С помощью regex ищем нужные места (после группы 1) и добавляем текущий год. 
    #Не забыть менять подстановочную строку каждый год. На стыке двух лет возможны ошибки

    df_sms_clean['Date'] = df_sms_date_temp[0] + df_sms_date_temp[1] #заполняем столбец с датой, соединяя строки
    df_sms_clean['Date'] = pd.to_datetime(df_sms_clean['Date'], errors='coerce') #меняем тип данных, принудительно пропуская ошибки. Строки в которых нет даты будут отображаться как NaT

    df_crm_clean = df_crm.drop(columns = ['Тип события', 'Наличие примечания', 'Дата/время', 'Дата/время.1'])
    df_crm_clean['Auth_datetime'] = pd.to_datetime(df_crm_clean['Дата/время.2'])
    df_crm_clean = df_crm_clean.rename({'Пользователь': 'Юзер. Событие', 'Пользователь.1': 'Юзер. Взаимодействие'}, axis=1)
    df_crm_clean = df_crm_clean.drop(columns = 'Дата/время.2')

    df_crm_clean = df_crm_clean.loc[(df_crm_clean['Состояние карты после обработки'] == 'Заблокирована') 
                                                 & (df_crm_clean['Признак мошенничества'] == 'НЕТ')
                                                 & ~(df_crm_clean['Типовой результат'].isin(['Нет телефона', 'Нет мобильного телефона']))]

    if len(df_sms_clean) != len(df_crm_clean):
        print('Количество СМС не совпадает с числом карт WARM CARD')#, 'SMS ', len(df_sms_clean), 'CRM ', len(df_crm_clean.loc[(df_crm_clean['Состояние карты после обработки'] == 'Заблокирована') & (df_crm_clean['Признак мошенничества'] == 'НЕТ')
    else:
        print('Количество СМС сходится с числом карт WARM CARD')

    #очистка ПАНов от примесей
    df_crm_clean['MaskPAN'] = [x[9:13] if len(x)>8 else x[3:8] for x in df_crm_clean['MaskPAN']]

    #недостающие СМС
    df_crm_clean['Телефон'] = df_crm_clean['Телефон'].astype(str).str[-10:]
    df_sms_clean['Телефон'] = df_sms_clean['Телефон'].astype(str).str[-10:]
    missing_CRM = df_sms_clean.merge(df_crm_clean, indicator=True, how='left', on = 'Телефон').loc[lambda x: x['_merge'] == 'left_only']
    missing_CRM.loc[missing_CRM['_merge'] == 'left_only', 'Комментарий'] = 'Нет в CRM'
    missing_CRM = missing_CRM.drop(columns = '_merge')
    missing_SMS = df_crm_clean.merge(df_sms_clean, indicator=True, how='left', on = 'Телефон').loc[lambda x: x['_merge'] == 'left_only']
    missing_SMS.loc[missing_SMS['_merge'] == 'left_only', 'Комментарий'] = 'Нет SMS'
    missing_SMS = missing_SMS.drop(columns = '_merge')

    sms_itog = missing_SMS.merge(missing_CRM, how = 'outer')
    sms_itog.to_csv('sms_itog.csv', header=True, index=False, encoding='cp1251')
    
    return [missing_SMS, missing_CRM]

def Start():
    uploader1 = widgets.FileUpload(multiple=True)
    print('Выберите файл(-ы) с sms (формат .csv):')
    display(uploader1)
    uploader2 = widgets.FileUpload(multiple=True)
    print('Выберите файл с выгрузкой из CRM (формат .xlsx):')
    display(uploader2)
    
    button = widgets.Button(description='Запустить ПК')
    out = widgets.Output()
    def on_button_clicked(_): #кнопка для загрузки файлов и запуска функции PK_SMS
        # "linking function with output"
        with out:
            # what happens when we press the button
            out.clear_output()
            uploaded_file1 = uploader1.value
            df_sms = pd.read_csv(io.BytesIO(uploaded_file1[0].content), sep=';', encoding='cp1251')
            for i in range(1,len(uploaded_file1),1):
                df_sms = pd.concat([df_sms, pd.read_csv(io.BytesIO(uploaded_file1[i].content), sep=';', encoding='cp1251')])
            df_sms=df_sms.drop(df_sms.columns[3], axis=1)

            uploaded_file2 = uploader2.value[0]
            df_crm = pd.read_excel(io.BytesIO(uploaded_file2.content), header=1, dtype={'Телефон': 'str'})
            result = PK_SMS(df_sms, df_crm)
            if result[0].empty and result[1].empty:
                print('Все sms соответствуют')
            if result[0].empty == False:
                print('\nSMS не отправлялись:')
                display(result[0])
            if result[1].empty == False:
                print('\nЗаписи об SMS отсутствуют в CRM:')
                display(result[1])
    # linking button and function together using a button's method
    button.on_click(on_button_clicked)
    print('После выбора файлов нажмите:')
    display(button)
    
    print('Результат ПК отобразится ниже после нажатия кнопки, а также будет сохранен в файле sms_itog.csv')
    display(out)
