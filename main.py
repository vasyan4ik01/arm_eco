import pandas as pd
import numpy as np
import os
from datetime import datetime
from collections import defaultdict

CONFIG_FILE = r'/home/cezar/Рабочий стол/auto_arm/config.xlsx'
NORMS_FILE = r'/home/cezar/Рабочий стол/auto_arm/normative.xlsx'
DATA_DIR = r'/home/cezar/Рабочий стол/arm_dev'
REPORT_DIR = r'/home/cezar/Рабочий стол/auto_arm/reports'

FILES_TO_PROCESS = {
    'Л2': os.path.join(DATA_DIR, 'Результаты испытатаний в точке Л2 (25).xlsx'),
    'В2': os.path.join(DATA_DIR, 'Результаты испытатаний в точке В2 (25).xlsx'),
    'Т2': os.path.join(DATA_DIR, 'Результаты испытатаний в точке Т2 (25).xlsx'),
}

def analyze_data(df, start_date, end_date, norms_dict, point_name):
    df = df[(df['Дата'] >= pd.to_datetime(start_date)) & (df['Дата'] <= pd.to_datetime(end_date))]
    df = df.reset_index(drop=True)

    result_rows = []

    # === ОБЫЧНАЯ ОБРАБОТКА ДЛЯ ВСЕХ КРОМЕ ФЕНОЛА ===
    for col in df.columns:
        if col in ['Дата', 'Время', 'Фенол']:
            continue  # Пропускаем дату, время и сам фенол (он ниже отдельно)
        
        try:
            values = pd.to_numeric(df[col].dropna(), errors='coerce').dropna()
        except Exception:
            continue

        if values.empty:
            continue

        lower, upper = norms_dict.get(col, (None, None))

        exceed_count = sum(
            (upper is not None and v > upper) or
            (lower is not None and v < lower)
            for v in values
        )

        result_rows.append({
            'Ингридиент': col,
            'Среднее значение': round(values.mean(), 3),
            'Мин значение': round(values.min(), 3),
            'Макс значение': round(values.max(), 3),
            'Норма нижняя': lower,
            'Норма верхняя': upper,
            'Кол-во анализов': len(values),
            'Кол-во превышений': exceed_count,
            'Процент превышений': round((exceed_count / len(values)) * 100, 3)
        })

    # === СПЕЦИАЛЬНАЯ ОБРАБОТКА ФЕНОЛА ПО ИЗМЕРЕНИЯМ ===
    if 'Фенол' in df.columns:
        df['Дата'] = pd.to_datetime(df['Дата'].ffill(), errors='coerce')
        df['DateOnly'] = df['Дата'].dt.date
        phenol_df = df[['DateOnly', 'Фенол']].copy()
        phenol_df = phenol_df.dropna()
        phenol_df['Фенол'] = pd.to_numeric(phenol_df['Фенол'], errors='coerce')
        grouped = phenol_df.groupby('DateOnly')['Фенол'].apply(list)

        max_measurements = 6
        lower, upper = norms_dict.get('Фенол', (None, None))

        for i in range(max_measurements):
            values = []
            for date, day_values in grouped.items():
                if len(day_values) > i:
                    values.append(day_values[i])
            values = pd.Series(values).dropna()

            if values.empty:
                continue

            exceed_count = sum(
                (upper is not None and v > upper) or
                (lower is not None and v < lower)
                for v in values
            )

            result_rows.append({
                'Ингридиент': 'Фенол',
                'Среднее значение': round(values.mean(), 3),
                'Мин значение': round(values.min(), 3),
                'Макс значение': round(values.max(), 3),
                'Норма нижняя': lower,
                'Норма верхняя': upper,
                'Кол-во анализов': len(values),
                'Кол-во превышений': exceed_count,
                'Процент превышений': round((exceed_count / len(values)) * 100, 3)
            })

    return pd.DataFrame(result_rows)



def save_report(df_result, point_name):
    today_str = datetime.today().strftime('%d.%m.%Y_%H-%M-%S')
    report_folder = os.path.join(os.path.dirname(REPORT_DIR), 'АРМ отчеты')
    os.makedirs(report_folder, exist_ok=True)
    report_path = os.path.join(report_folder, f"{point_name} отчет от {today_str}.xlsx")
    df_result.to_excel(report_path, index=False)

def main():
    os.makedirs(REPORT_DIR, exist_ok=True)

    # Чтение периода из config.xlsx
    config = pd.read_excel(CONFIG_FILE)
    start_date = config['Дата_начала'][0].date()
    end_date = config['Дата_окончания'][0].date()

    # Чтение нормативов
    norms = pd.read_excel(NORMS_FILE)
    norms_dicts = defaultdict(dict)
    for _, row in norms.iterrows():
        point = row['Точка']
        param = row['Параметр']
        lower = row['Нижняя_граница'] if not pd.isna(row['Нижняя_граница']) else None
        upper = row['Верхняя_граница'] if not pd.isna(row['Верхняя_граница']) else None
        norms_dicts[point][param] = (lower, upper)

    # Загрузка данных с учётом специфики строк
    data_frames = {
        'Л2': pd.read_excel(FILES_TO_PROCESS['Л2'], sheet_name='2025', header=1, parse_dates=['Дата'])[1:],
        'В2': pd.read_excel(FILES_TO_PROCESS['В2'], sheet_name='2025', header=1, parse_dates=['Дата'])[1:],
        'Т2': pd.read_excel(FILES_TO_PROCESS['Т2'], sheet_name='2025', header=1, parse_dates=['Дата'])[4:]
    }

    for point_name, df in data_frames.items():
        df['Дата'] = pd.to_datetime(df['Дата'].ffill(), errors='coerce')
        result_df = analyze_data(df, start_date, end_date, norms_dicts.get(point_name, {}), point_name)
        save_report(result_df, point_name)

if __name__ == "__main__":
    main()
