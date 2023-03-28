import pandas as pd
import requests
import MySQLdb as mdb

# Устанавливаем соединение с базой данных
my_host = 'localhost'
my_user = 'your_username'
my_password = 'your_password'
my_database = 'your_database'
my_charset = 'utf8'

con = mdb.connect(host=my_host, user=my_user, password=my_password, database=my_database,
                  charset=my_charset)

# Берём данные по API
Authorization='your_WB_API_key'
url_stocks = 'https://statistics-api.wildberries.ru/api/v1/supplier/stocks'
headers = {'Authorization': Authorization}
param_request = {'dateFrom': '2018-01-01'}
response_stocks = requests.get(url_stocks, headers=headers, params=param_request)
wb_json_stocks = response_stocks.content

# Наши данные
df = pd.read_json(wb_json_stocks)

# Если новых данных нет, то пропускаем
if df.shape[0] == 0:
    pass
else:
    # Убираем лишние колонки
    df.drop(columns=['isSupply', 'isRealization', 'SCCode'], inplace=True)

    # Приводим lastChangeDate к формату даты
    df['lastChangeDate'] = pd.to_datetime(df['lastChangeDate'])

    # Создаем курсор для работы с базой данных
    cur = con.cursor()

    # Удалим все предыдущие значения в таблице stocks
    delete_table_values_query = "DELETE FROM stocks"

    try:
        cur.execute(delete_table_values_query)
        con.commit()
        print("Все записи удалены успешно")
    except:
        con.rollback()
        print("Ошибка удаления записей")

    # Определяем запрос для создания таблицы
    create_table_query = '''CREATE TABLE IF NOT EXISTS stocks (
                            lastChangeDate DATETIME,
                            supplierArticle VARCHAR(255) CHARACTER SET utf8,
                            techSize VARCHAR(255) CHARACTER SET utf8,
                            barcode VARCHAR(255) CHARACTER SET utf8,
                            quantity INT,
                            quantityFull INT,
                            warehouseName VARCHAR(255) CHARACTER SET utf8,
                            nmId INT,
                            subject VARCHAR(255) CHARACTER SET utf8,
                            category VARCHAR(255) CHARACTER SET utf8,
                            daysOnSite INT,
                            brand VARCHAR(255) CHARACTER SET utf8,
                            Price INT,
                            Discount INT
                            );'''

    # Создаем таблицу в базе данных
    cur.execute(create_table_query)

    # Определяем запрос для добавления данных в таблицу
    insert_data_query = '''INSERT INTO stocks  (lastChangeDate,
                                                supplierArticle,
                                                techSize,
                                                barcode,
                                                quantity,
                                                quantityFull,
                                                warehouseName,
                                                nmId,
                                                subject,
                                                category,
                                                daysOnSite,
                                                brand,
                                                Price,
                                                Discount) 
                           VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);'''

    # Перебираем строки датафрейма и добавляем их в базу данных
    for i in df.index:
        lastChangeDate = df.at[i, 'lastChangeDate'].strftime('%Y-%m-%d %H:%M:%S')
        supplierArticle = df.at[i, 'supplierArticle']
        techSize = df.at[i, 'techSize']
        barcode = df.at[i, 'barcode']
        quantity = df.at[i, 'quantity']
        quantityFull = df.at[i, 'quantityFull']
        warehouseName = df.at[i, 'warehouseName']
        nmId = df.at[i, 'nmId']
        subject = df.at[i, 'subject']
        category = df.at[i, 'category']
        daysOnSite = df.at[i, 'daysOnSite']
        brand = df.at[i, 'brand']
        Price = df.at[i, 'Price']
        Discount = df.at[i, 'Discount']

        cur.execute(insert_data_query,
                    (lastChangeDate, supplierArticle, techSize, barcode, quantity, quantityFull, warehouseName,
                     nmId, subject, category, daysOnSite, brand, Price, Discount))

    # Сохраняем изменения в базе данных
    con.commit()

# Закрываем соединение с базой данных
con.close()
