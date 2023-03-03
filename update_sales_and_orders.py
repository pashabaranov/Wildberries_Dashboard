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

# Чистим таблицы, куда будем загружать данные
client.open('Orders_and_Sales_table').values_clear('Sheet1')
client.open('Sales_table').values_clear('Sheet1')

# Будем загружать данные в эти таблицы
sheet_orders_and_sale = client.open('Orders_and_Sales_table').sheet1
sheet_sale = client.open('Sales_table').sheet1

# Берём данные по API (продажи)
Authorization = 'your_wildberries_API_key'
url_sales = 'https://statistics-api.wildberries.ru/api/v1/supplier/sales'
headers = {'Authorization': Authorization}
param_request = {'dateFrom': '2019-12-01'}
response_sales = requests.get(url_sales, headers=headers, params=param_request)
wb_json_sales = response_sales.content

# Берём данные по API (заказы)
url_orders = 'https://statistics-api.wildberries.ru/api/v1/supplier/orders'
headers = {'Authorization': Authorization}
param_request = {'dateFrom': '2019-12-01'}
response_orders = requests.get(url_orders, headers=headers, params=param_request)
wb_json_orders = response_orders.content

# Наши исходные данные
sales = pd.read_json(wb_json_sales)
orders = pd.read_json(wb_json_orders)

# Дубликат данных по продажам (они будут в отдельной таблице)
sales_origin = pd.read_json(wb_json_sales)

# 1. Соединим данные о продажах и заказах в один датафрейм
# Рассчитаем стоимость заказанного товара с учётом скидки
orders['finishedPrice'] = orders['totalPrice'] * (1 - orders['discountPercent'] / 100)

# Добавим недостающие колонки в таблицу orders
orders['status'] = 'order'
orders['quantity'] = 1


# Функция для определения статуса в таблице sales
def defined_status_sales(col):
    if col.startswith('S'):
        return 'sale'
    elif col.startswith('R'):
        return 'return'
    elif col.startswith('D'):
        return 'surcharge'
    elif col.startswith('A'):
        return 'storno_sales'
    elif col.startswith('B'):
        return 'storno_return'


# Добавим недостающие колонки в таблицу sales
sales['status'] = sales['saleID'].apply(lambda x: defined_status_sales(x))
sales['quantity'] = 1

# Соединим таблицы sales и orders
orders_and_sales = pd.concat(
    [sales[['date', 'supplierArticle', 'techSize', 'finishedPrice', 'status', 'quantity', 'category', 'forPay']],
     orders[['date', 'supplierArticle', 'techSize', 'finishedPrice', 'status', 'quantity', 'category']]])

# Приводим колонку date к формату даты (timedelta) и отсортируем по возрастанию
orders_and_sales['date'] = pd.to_datetime(orders_and_sales['date'])
orders_and_sales = orders_and_sales.sort_values('date')

# Сбросим индексы
orders_and_sales.reset_index(inplace=True)
orders_and_sales.drop(columns='index', inplace=True)

# Приводим дату к формату yyyy-mm-dd и типу str
orders_and_sales['date'] = orders_and_sales['date'].dt.date
orders_and_sales['date'] = orders_and_sales['date'].apply(str)

# Заполняем NoneType значения в колонке forPay (к перечислению поставщику) нулями
orders_and_sales['forPay'] = orders_and_sales['forPay'].fillna(0)

# Заполняем остальные NoneType значения (во избежание ошибки)
orders_and_sales = orders_and_sales.fillna('')

# 2. Очистим исходные данные о продажах (для определённых элементов дашборда)
# С помощью предыдущей функции определим статус покупки (для удобства)
sales_origin['saleID_status'] = sales_origin.saleID.apply(lambda x: defined_status_sales(x))

# Убираем ненужные колонки
sales_origin = sales_origin[['date', 'supplierArticle', 'regionName', 'saleID_status']]

# Приводим дату к формату yyyy-mm-dd и типу str
sales_origin['date'] = pd.to_datetime(sales_origin['date'])
sales_origin['date'] = sales_origin['date'].dt.date
sales_origin['date'] = sales_origin['date'].apply(str)

# Заполняем NoneType значения (во избежание ошибки)
sales_origin = sales_origin.fillna('')

# Добавляем в гугл таблицы новые данные
sheet_orders_and_sale.update([orders_and_sales.columns.values.tolist()] + orders_and_sales.values.tolist())
sheet_sale.update([sales_origin.columns.values.tolist()] + sales_origin.values.tolist())
