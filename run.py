import pandas as pd
import numpy as np
import os
import re
from datetime import datetime
from collections import defaultdict

CONFIG_FILE = r'/home/cezar/Рабочий стол/auto_arm/config.xlsx'
NORMS_FILE = r'/home/cezar/Рабочий стол/auto_arm/normative.xlsx'
DATA_DIR = r'/home/cezar/Рабочий стол/arm_dev'
REPORT_DIR = r'/home/cezar/Рабочий стол/auto_arm/reports'

FILES_TO_PROCESS = {
    'Л2': '/home/cezar/Рабочий стол/arm_dev/Результаты испытатаний в точке Л2 (25).xlsx',
    'В2': '/home/cezar/Рабочий стол/arm_dev/Результаты испытатаний в точке В2 (25).xlsx',
    'Т2': '/home/cezar/Рабочий стол/arm_dev/Результаты испытатаний в точке Т2 (25).xlsx',
}
FILE_SETTINGS = {
    'Л2': {'header_row': 2, 'data_start_row': 3, 'normative_row': None},
    'В2': {'header_row': 2, 'data_start_row': 3, 'normative_row': None},
    'Т2': {'header_row': 2, 'data_start_row': 6, 'normative_row': 5},    
}

def main():
    os.makedirs(REPORT_DIR, exist_ok=True)
        #loading config faila. vnutr'await func main
    
    config = pd.read_excel(CONFIG_FILE)
    start_date = config['Дата_начала'][0].date()
    end_date = config['Дата_окончания'][0].date()   
    # config  #Он реверсит дату в гггг.мм.дд
    
    norms = pd.read_excel(NORMS_FILE)
    norms_dict = {} 
    for _, row in norms.iterrows():
        param = row['Параметр']
        lower = row['Нижняя_граница'] if not pd.isna(row['Нижняя_граница']) else None
        upper = row['Верхняя_граница'] if not pd.isna(row['Верхняя_граница']) else None
        norms_dict[param] = (lower, upper) #в итоге если есть верхяя граница, то храним два значения(Вроде), лтбо пару значени и нан
    # norms
    # norms_dict 
    
    df_l2 = pd.read_excel(FILES_TO_PROCESS.get('Л2'), sheet_name='2025', header=1, parse_dates=['Дата'])[1:] #дропамем первую строку!!!!!
    df_l2['Дата'] = pd.to_datetime(df_l2['Дата'].ffill(), errors = 'coerce')

    df_v2 = pd.read_excel(FILES_TO_PROCESS.get('В2'), sheet_name='2025', header=1, parse_dates=['Дата'])[1:] #дропамем первую строку!!!!!
    df_v2['Дата'] = pd.to_datetime(df_v2['Дата'].ffill(), errors = 'coerce')

    #В т2 другая шапка. считать нормы и новое начало
    df_t2 = pd.read_excel(FILES_TO_PROCESS.get('Т2'), sheet_name='2025', header=1, parse_dates=['Дата'])[4:]
    df_t2['Дата'] = pd.to_datetime(df_t2['Дата'].ffill(), errors = 'coerce')
    
    
def analyze_data(df, start_date, end_date, norms_dict, point_name):
    df = df[(df['Дата'] >= pd.to_datetime(start_date)) & (df['Дата'] <= pd.to_datetime(end_date))]
    df = df.reset_index(drop=True)
    
    result_rows = []
    for col in df.columns:
        if col == 'Дата':
            continue
        values = df[col].dropna().astype(float)

        if values.empty:
            continue

        lower, upper = norms_dict.get(col, (None, None))

        exceed_count = 0
        for v in values:
            if upper is not None and v > upper:
                exceed_count += 1
            elif lower is not None and v < lower:
                exceed_count += 1

        result_rows.append({
            'Ингридиент': col,
            'Среднее значение': round(values.mean(), 2),
            'Мин значение': round(values.min(), 2),
            'Макс значение': round(values.max(), 2),
            'Норма нижняя': lower,
            'Норма верхняя': upper,
            'Кол-во анализов': len(values),
            'Кол-во превышений': exceed_count,
            'Процент превышений': round((exceed_count / len(values)) * 100, 2)
        })

    return pd.DataFrame(result_rows)

def save_report(df_result, point_name):
    today_str = datetime.today().strftime('%d.%m.%Y')
    report_folder = os.path.join(os.path.dirname(REPORT_DIR), 'АРМ отчеты')
    os.makedirs(report_folder, exist_ok=True)
    report_path = os.path.join(report_folder, f"{point_name} отчет от {today_str}.xlsx")
    df_result.to_excel(report_path, index=False)

def main():
    os.makedirs(REPORT_DIR, exist_ok=True)

    config = pd.read_excel(CONFIG_FILE)
    start_date = config['Дата_начала'][0].date()
    end_date = config['Дата_окончания'][0].date()

    norms = pd.read_excel(NORMS_FILE)
    norms_dicts = defaultdict(dict)
    for _, row in norms.iterrows():
        point = row['Точка']
        param = row['Параметр']
        lower = row['Нижняя_граница'] if not pd.isna(row['Нижняя_граница']) else None
        upper = row['Верхняя_граница'] if not pd.isna(row['Верхняя_граница']) else None
        norms_dicts[point][param] = (lower, upper)

    data_frames = {
        'Л2': pd.read_excel(FILES_TO_PROCESS['Л2'], sheet_name='2025', header=1, parse_dates=['Дата'])[1:],
        'В2': pd.read_excel(FILES_TO_PROCESS['В2'], sheet_name='2025', header=1, parse_dates=['Дата'])[1:],
        'Т2': pd.read_excel(FILES_TO_PROCESS['Т2'], sheet_name='2025', header=1, parse_dates=['Дата'])[4:]
    }

    for point_name, df in data_frames.items():
        df['Дата'] = pd.to_datetime(df['Дата'].ffill(), errors='coerce')
        result_df = analyze_data(df, start_date, end_date, norms_dicts.get(point_name, {}), point_name)
        save_report(result_df, point_name)
