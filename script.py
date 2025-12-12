import re
import requests
from bs4 import BeautifulSoup
import pandas as pd
import json
import time
from kafka import KafkaProducer

URL = "https://en.wikipedia.org/wiki/List_of_highest-grossing_films"
KAFKA_TOPIC = "bonus_22B22B1510"
KAFKA_BROKER = 'localhost:9092' 

def scrape_data(url):
    """Скрейпинг статической страницы и преобразование в DataFrame."""
    print(f"1. Запуск скрейпинга с: {url}")
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    
    try:
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status() # Проверка на ошибки HTTP
    except requests.exceptions.HTTPError as e:
        print(f"Ошибка HTTP: {e}. Возможно, сайт продолжает блокировать запрос.")
        return pd.DataFrame()
    except requests.exceptions.RequestException as e:
        print(f"Общая ошибка при запросе к URL: {e}")
        return pd.DataFrame()

    soup = BeautifulSoup(response.text, 'html.parser')
    
    all_tables = soup.find_all('table', {'class': 'wikitable'})
    
    if len(all_tables) > 0:
        table = all_tables[0] 
    else:
        table = None
    
    if not table:
        print("Таблица с фильмами не найдена. Проверьте URL или структуру HTML.")
        return pd.DataFrame()

    headers = [th.text.strip().split('[')[0] for th in table.find('tr').find_all('th')]
    
    data = []
    for row in table.find_all('tr')[1:]:
        cells = row.find_all(['th', 'td'])
        row_data = [cell.text.strip() for cell in cells]
        if len(row_data) >= 5: 
            data.append([row_data[0], row_data[2], row_data[3], row_data[4]])
    
    df = pd.DataFrame(data, columns=['Rank_Raw', 'Title_Raw', 'Gross_Raw', 'Year_Raw'])
    
    print(f"Скрейпинг успешно выполнен. Извлечено {len(df)} строк.")
    return df

def clean_data(df):
    """
    Очистка данных с использованием регулярных выражений для надежности.
    """
    if df.empty:
        return df

    print("2. Запуск очистки данных...")

    df = df.rename(columns={
        'Rank_Raw': 'Rank',
        'Title_Raw': 'Title',
        'Gross_Raw': 'Gross_USD',
        'Year_Raw': 'Release_Year'
    })
    
    df['Title'] = df['Title'].apply(lambda x: re.sub(r'\[.*?\]', '', x).strip())
    
    def clean_gross(gross):
        numeric_part = re.sub(r'[^\d\.]', '', gross.replace(',', ''))
        try:
            return float(numeric_part) if numeric_part else None
        except ValueError:
            return None
    
    df['Gross_USD'] = df['Gross_USD'].apply(clean_gross)
    
    def clean_year(year):
        match = re.search(r'(\d{4})', year)
        if match:
            return int(match.group(1))
        return None

    df['Release_Year'] = df['Release_Year'].apply(clean_year).astype('Int64')

    df['Rank'] = df['Rank'].apply(lambda x: pd.to_numeric(re.search(r'(\d+)', x).group(1), errors='coerce') if re.search(r'(\d+)', x) else None).astype('Int64')
    
    df_cleaned = df.dropna(subset=['Rank', 'Gross_USD', 'Release_Year']).copy()

    print(f"Очистка завершена. Осталось {len(df_cleaned)} валидных строк.")
    return df_cleaned[['Rank', 'Title', 'Gross_USD', 'Release_Year']]

def produce_to_kafka(df, topic, broker):
    """Отправка каждой строки DataFrame в топик Kafka как JSON-сообщение."""
    if df.empty:
        print("3. DataFrame пуст, отправка в Kafka пропущена.")
        return

    print(f"3. Отправка {len(df)} сообщений в топик Kafka: {topic}...")
    
    try:
        producer = KafkaProducer(
            bootstrap_servers=[broker],
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        print("Успешное подключение к Kafka.")

        for index, row in df.iterrows():
            message = row.to_dict()
            
            producer.send(topic, value=message)
            

        producer.flush()
        print(f"Успешно отправлено {len(df)} сообщений в Kafka.")

    except Exception as e:
        print(f"Критическая ошибка при работе с Kafka. Проверьте, запущен ли брокер на {broker}.")
        print(f"Ошибка: {e}")

def save_data(df, filename):
    """Сохранение очищенного DataFrame в CSV-файл."""
    df.to_csv(filename, index=False)
    print(f"4. Очищенный набор данных сохранен в {filename}")

if __name__ == "__main__":
    raw_df = scrape_data(URL)
    
    cleaned_df = clean_data(raw_df)
    
    SAVE_FILE = "cleaned_data.csv"
    save_data(cleaned_df, SAVE_FILE)
    
    produce_to_kafka(cleaned_df, KAFKA_TOPIC, KAFKA_BROKER)