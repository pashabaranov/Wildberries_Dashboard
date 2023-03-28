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

# Определим последнюю дату и время обновления данных
query = "SELECT MAX(lastChangeDate) as last_date FROM orders"
df_sql = pd.read_sql(query, con)
last_date = df_sql['last_date'][0]

# Если SQL таблицы нет, то выполняем это, вместо предыдущих строк
# last_date = '2018-01-01'

# Берём данные по API
Authorization='your_WB_API_key'
url_orders = 'https://statistics-api.wildberries.ru/api/v1/supplier/orders'
headers = {'Authorization': Authorization}
param_request = {'dateFrom': last_date}
response_orders = requests.get(url_orders, headers=headers, params=param_request)
wb_json_orders = response_orders.content

# Наши данные
df = pd.read_json(wb_json_orders)

# Если новых данных нет, то пропускаем
if df.shape[0] == 0:
    pass
else:
    # Рассчитываем стоимость товара для покупателя с дисконтом
    df['priceWithDiscount'] = df['totalPrice'] * (1 - df['discountPercent'] / 100)

    # Приводим lastChangeDate к формату даты
    df['lastChangeDate'] = pd.to_datetime(df['lastChangeDate'])

    # Приводим barcode к формату object
    df['barcode'] = df['barcode'].astype('object')

    # Создаем курсор для работы с базой данных
    cur = con.cursor()

    # Определяем запрос для создания таблицы
    create_table_query = '''CREATE TABLE IF NOT EXISTS orders (
                            date DATE,
                            lastChangeDate DATETIME,
                            supplierArticle VARCHAR(255) CHARACTER SET utf8,
                            techSize VARCHAR(255) CHARACTER SET utf8,
                            barcode VARCHAR(255) CHARACTER SET utf8,
                            totalPrice FLOAT,
                            discountPercent INT,
                            warehouseName VARCHAR(255) CHARACTER SET utf8,
                            oblast VARCHAR(255) CHARACTER SET utf8,
                            incomeID INT,
                            odid INT,
                            nmId INT,
                            subject VARCHAR(255) CHARACTER SET utf8,
                            category VARCHAR(255) CHARACTER SET utf8,
                            brand VARCHAR(255) CHARACTER SET utf8,
                            isCancel BOOL,
                            cancel_dt VARCHAR(255) CHARACTER SET utf8,
                            gNumber FLOAT,
                            sticker VARCHAR(255) CHARACTER SET utf8,
                            srid VARCHAR(255) CHARACTER SET utf8,
                            priceWithDiscount FLOAT
                            );'''

    # Создаем таблицу в базе данных
    cur.execute(create_table_query)

    # Определяем запрос для добавления данных в таблицу
    insert_data_query = '''INSERT INTO orders  (date,
                                                lastChangeDate,
                                                supplierArticle,
                                                techSize,
                                                barcode,
                                                totalPrice,
                                                discountPercent,
                                                warehouseName,
                                                oblast,
                                                incomeID,
                                                odid,
                                                nmId,
                                                subject,
                                                category,
                                                brand,
                                                isCancel,
                                                cancel_dt,
                                                gNumber,
                                                sticker,
                                                srid,
                                                priceWithDiscount) 
                           VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);'''

    # Перебираем строки датафрейма и добавляем их в базу данных
    for i in df.index:
        date = df.at[i, 'date'].strftime('%Y-%m-%d')
        lastChangeDate = df.at[i, 'lastChangeDate'].strftime('%Y-%m-%d %H:%M:%S')
        supplierArticle = df.at[i, 'supplierArticle']
        techSize = df.at[i, 'techSize']
        barcode = df.at[i, 'barcode']
        totalPrice = df.at[i, 'totalPrice']
        discountPercent = df.at[i, 'discountPercent']
        warehouseName = df.at[i, 'warehouseName']
        oblast = df.at[i, 'oblast']
        incomeID = df.at[i, 'incomeID']
        odid = df.at[i, 'odid']
        nmId = df.at[i, 'nmId']
        subject = df.at[i, 'subject']
        category = df.at[i, 'category']
        brand = df.at[i, 'brand']
        isCancel = df.at[i, 'isCancel']
        cancel_dt = df.at[i, 'cancel_dt']
        gNumber = df.at[i, 'gNumber']
        sticker = df.at[i, 'sticker']
        srid = df.at[i, 'srid']
        priceWithDiscount = df.at[i, 'priceWithDiscount']

        cur.execute(insert_data_query,
                    (date, lastChangeDate, supplierArticle, techSize, barcode, totalPrice, discountPercent, warehouseName,
                     oblast, incomeID, odid, nmId, subject, category, brand, isCancel, cancel_dt, gNumber, sticker, srid,
                     priceWithDiscount))

    # Сохраняем изменения в базе данных
    con.commit()

# Закрываем соединение с базой данных
con.close()
