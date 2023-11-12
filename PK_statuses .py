import pandas as pd
import numpy as np
import re
import os
import ipywidgets as widgets
from ipywidgets import HBox, VBox
import io
from pandas import ExcelWriter

def PK_statuses(df_owi, df_crm): 
        df_owi_clean = df_owi.drop(columns = 'Сервис')
        df_owi_clean['Дата изменения'] = pd.to_datetime(df_owi_clean['Дата изменения'])
        df_owi_clean = df_owi_clean.loc[df_owi_clean['Новое значение статуса'] != '50 (AUTO LOCKED CARD)']
        df_owi_clean.loc[df_owi_clean['Новое значение статуса'] == '2 (WARM CARD)', 'UTRNNO'] = df_owi_clean['Комментарий'].str.slice(start=-11)

        df_crm_clean = df_crm.drop(columns = ['Тип события', 'Наличие примечания', 'Дата/время', 'Дата/время.1'])
        df_crm_clean['Auth_datetime'] = pd.to_datetime(df_crm_clean['Дата/время.2'])
        df_crm_clean = df_crm_clean.rename({'Пользователь': 'Юзер. Событие', 'Пользователь.1': 'Юзер. Взаимодействие'}, axis=1)
        df_crm_clean = df_crm_clean.drop(columns = 'Дата/время.2')

        df_owi_clean = df_owi_clean.sort_values(by=['PAN','Дата изменения'], ascending=False)
        df_owi_clean.loc[df_owi_clean['PAN'] == df_owi_clean['PAN'].shift(-1), 'UTRNNO'] = df_owi_clean['UTRNNO'].fillna(df_owi_clean['UTRNNO'].shift(-1))
            
        df_owi_clean = df_owi_clean.groupby('PAN', group_keys = False).apply(lambda x: x.nlargest(1, 'Дата изменения'))
        
        try:
            df_status_utrnno_ok = pd.merge(df_owi_clean.astype({'UTRNNO': np.int64}), df_crm_clean, how='inner', on='UTRNNO')

            cond_for_nok_statuses = ((df_status_utrnno_ok['Новое значение статуса'] == '0 (VALID CARD)') & (df_status_utrnno_ok['Состояние карты после обработки'] != 'Активна'))|((df_status_utrnno_ok['Новое значение статуса'] == '2 (WARM CARD)') & ((df_status_utrnno_ok['Признак мошенничества'] != 'НЕТ') | (df_status_utrnno_ok['Состояние карты после обработки'] != 'Заблокирована')))|((df_status_utrnno_ok['Новое значение статуса'] == '10 (PICK UP, SPECIAL CONDITION)') & ((df_status_utrnno_ok['Признак мошенничества'] != 'ДА') | (df_status_utrnno_ok['Состояние карты после обработки'] != 'Заблокирована')))
            df_status_nok = df_status_utrnno_ok.loc[cond_for_nok_statuses]

            with ExcelWriter('result.xlsx', engine="openpyxl", mode='w') as writer:
                df_status_nok.to_excel(writer, sheet_name="Not_Ok_Status", index = False)

            df_owi_missed = df_owi_clean[~df_owi_clean['UTRNNO'].astype({'UTRNNO': np.int64}).isin(df_status_utrnno_ok['UTRNNO'])]
            with ExcelWriter('result.xlsx', engine="openpyxl", mode='a') as writer:
                df_owi_missed.to_excel(writer, sheet_name="OWI_Only", index = False)

            df_crm_missed = df_crm_clean[~df_crm_clean['UTRNNO'].isin(df_status_utrnno_ok['UTRNNO'])]
            with ExcelWriter('result.xlsx', engine="openpyxl", mode='a') as writer:
                df_crm_missed.to_excel(writer, sheet_name="CRM_Only", index = False)

            return [df_status_nok, df_owi_missed, df_crm_missed]
      
        except ValueError:
            print('\nОшибка в выгрузке из OWI. Поле UTRNNO содержит пустое или некорректное значение:')
            df_error = df_owi_clean[(df_owi_clean['UTRNNO'].isna())|~df_owi_clean['UTRNNO'].str[-1].isin(['0','1','2','3','4','5','6','7','8','9'])]
            display(df_error)
            df_owi_clean = df_owi_clean[~df_owi_clean['UTRNNO'].isin(df_error['UTRNNO'])]
            df_status_utrnno_ok = pd.merge(df_owi_clean.astype({'UTRNNO': np.int64}), df_crm_clean, how='inner', on='UTRNNO')

            cond_for_nok_statuses = ((df_status_utrnno_ok['Новое значение статуса'] == '0 (VALID CARD)') & (df_status_utrnno_ok['Состояние карты после обработки'] != 'Активна'))|((df_status_utrnno_ok['Новое значение статуса'] == '2 (WARM CARD)') & ((df_status_utrnno_ok['Признак мошенничества'] != 'НЕТ') | (df_status_utrnno_ok['Состояние карты после обработки'] != 'Заблокирована')))|((df_status_utrnno_ok['Новое значение статуса'] == '10 (PICK UP, SPECIAL CONDITION)') & ((df_status_utrnno_ok['Признак мошенничества'] != 'ДА') | (df_status_utrnno_ok['Состояние карты после обработки'] != 'Заблокирована')))
            df_status_nok = df_status_utrnno_ok.loc[cond_for_nok_statuses]

            with ExcelWriter('result.xlsx', engine="openpyxl", mode='w') as writer:
                df_status_nok.to_excel(writer, sheet_name="Not_Ok_Status", index = False)

            df_owi_missed = df_owi_clean[~df_owi_clean['UTRNNO'].astype({'UTRNNO': np.int64}).isin(df_status_utrnno_ok['UTRNNO'])]
            with ExcelWriter('result.xlsx', engine="openpyxl", mode='a') as writer:
                df_owi_missed.to_excel(writer, sheet_name="OWI_Only", index = False)

            df_crm_missed = df_crm_clean[~df_crm_clean['UTRNNO'].isin(df_status_utrnno_ok['UTRNNO'])]
            with ExcelWriter('result.xlsx', engine="openpyxl", mode='a') as writer:
                df_crm_missed.to_excel(writer, sheet_name="CRM_Only", index = False)
                
            with ExcelWriter('result.xlsx', engine="openpyxl", mode='a') as writer:
                df_error.to_excel(writer, sheet_name="Error", index = False)

            return [df_status_nok, df_owi_missed, df_crm_missed]
    
def Start():
    uploader1 = widgets.FileUpload(multiple=True)
    print('Выберите файл с выгрузкой из OWI (формат .csv):')
    display(uploader1)
    uploader2 = widgets.FileUpload(multiple=True)
    print('Выберите файл с выгрузкой из CRM (формат .xlsx):')
    display(uploader2)
    button = widgets.Button(description='Запустить ПК')
    out = widgets.Output()
    def on_button_clicked(_):
        # "linking function with output"
        with out:
            # what happens when we press the button
            out.clear_output()
            uploaded_file1 = uploader1.value[0]
            uploaded_file2 = uploader2.value[0]
            df_owi = pd.read_csv(io.BytesIO(uploaded_file1.content), sep=';', encoding='cp1251')
            df_crm = pd.read_excel(io.BytesIO(uploaded_file2.content), header=1)
            result = PK_statuses(df_owi, df_crm)
            print('Статусы в OWI и CRM не соответствуют:')
            display(result[0])
            print('Для записи в OWI нет соответствия в CRM по UTRNNO:')
            display(result[1])
            print('Для записи в CRM нет соответствия в OWI по UTRNNO:')
            display(result[2])
    # linking button and function together using a button's method
    button.on_click(on_button_clicked)
    print('После выбора файлов нажмите:')
    display(button)
    print('Результат ПК отобразится ниже после нажатия кнопки, а также будет сохранен в файле result.xlsx')
    display(out)




