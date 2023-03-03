import gspread
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd
import requests

# Ссылки, к которым нам нужен доступ
scope = ['https://www.googleapis.com/auth/spreadsheets',
         'https://www.googleapis.com/auth/drive']

# Читаем ключи из json (credentials)
credentials = ServiceAccountCredentials.from_json_keyfile_name('gs_credentials.json', scope)

# Авторизация в проектной почте
client = gspread.authorize(credentials)

# Чистим таблицу, куда будет загружать данные (во избежание багов)
client.open('Stocks_table').values_clear('Sheet1')

# Будем загружать данные в эту таблицу
sheet = client.open('Stocks_table').sheet1

# Берём данные по API
Authorization = 'your_wildberries_API_key'
url_stocks = 'https://statistics-api.wildberries.ru/api/v1/supplier/stocks'
headers = {'Authorization': Authorization}
param_request = {'dateFrom': '2019-12-01'}
response_stocks = requests.get(url_stocks, headers=headers, params=param_request)
wb_json_stocks = response_stocks.content

# Наши исходные данные
stocks = pd.read_json(wb_json_stocks)

# Убираем ненужные колонки
stocks.drop(columns=['lastChangeDate', 'barcode', 'nmId', 'subject', 'daysOnSite', 'brand', 'SCCode', 'Price', 'Discount'], inplace=True)

# Заполняем NoneType значения (во избежание ошибки)
stocks = stocks.fillna('')

# Добавляем в гугл таблицу новые данные
sheet.update([stocks.columns.values.tolist()] + stocks.values.tolist())
