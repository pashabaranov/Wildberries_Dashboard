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
client.open('Incomes_table').values_clear('Sheet1')

# Будем загружать данные в эту таблицу
sheet = client.open('Incomes_table').sheet1

# Берём данные по API
Authorization = 'your_wildberries_API_key'
url_incomes = 'https://statistics-api.wildberries.ru/api/v1/supplier/incomes'
headers = {'Authorization': Authorization}
param_request = {'dateFrom': '2019-12-01'}
response_incomes = requests.get(url_incomes, headers=headers, params=param_request)
wb_json_incomes = response_incomes.content

# Наши исходные данные
incomes = pd.read_json(wb_json_incomes)

# Убираем ненужные колонки
incomes.drop(columns=['number', 'lastChangeDate', 'barcode', 'totalPrice', 'nmId'], inplace=True)

# Приводим дату к формату yyyy-mm-dd и типу str
incomes['date'] = pd.to_datetime(incomes['date'])
incomes['date'] = incomes['date'].dt.date
incomes['date'] = incomes['date'].apply(str)

# Повторяем предыдущий шаг для колонки dateClose
incomes['dateClose'] = pd.to_datetime(incomes['dateClose'])
incomes['dateClose'] = incomes['dateClose'].dt.date
incomes['dateClose'] = incomes['dateClose'].apply(str)

# Заполняем NoneType значения (во избежание ошибки)
incomes = incomes.fillna('')

# Добавляем в гугл таблицу новые данные
sheet.update([incomes.columns.values.tolist()] + incomes.values.tolist())
