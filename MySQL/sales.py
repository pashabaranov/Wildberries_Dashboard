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
query = "SELECT MAX(lastChangeDate) as last_date FROM sales"
df_sql = pd.read_sql(query, con)
last_date = df_sql['last_date'][0]

# Если SQL таблицы нет, то выполняем это, вместо предыдущих строк
# last_date = '2018-01-01'

# Берём данные по API
Authorization = 'your_WB_API_key'
url_sales = 'https://statistics-api.wildberries.ru/api/v1/supplier/sales'
headers = {'Authorization': Authorization}
param_request = {'dateFrom': last_date}
response_sales = requests.get(url_sales, headers=headers, params=param_request)
wb_json_sales = response_sales.content

# Наши данные
df = pd.read_json(wb_json_sales)

# Если новых данных нет, то пропускаем
if df.shape[0] == 0:
    pass
else:
    # Приводим lastChangeDate к формату даты
    df['lastChangeDate'] = pd.to_datetime(df['lastChangeDate'])

    # Приводим barcode к формату object
    df['barcode'] = df['barcode'].astype('object')

    # Статус продаж
    def defined_status_sales(col):
        if col.startswith('S') == True:
            return 'sale'
        elif col.startswith('R') == True:
            return 'return'
        elif col.startswith('D') == True:
            return 'surcharge'
        elif col.startswith('A') == True:
            return 'storno_sales'
        elif col.startswith('B') == True:
            return 'storno_return'

    df['saleID_status'] = df['saleID'].apply(lambda x: defined_status_sales(x))

    # Убираем лишние колонки
    df.drop(columns=['isSupply', 'isRealization', 'saleID', 'promoCodeDiscount', 'IsStorno'], inplace=True)

    # Создаем курсор для работы с базой данных
    cur = con.cursor()

    # Определяем запрос для создания таблицы
    create_table_query = '''CREATE TABLE IF NOT EXISTS sales (
                            date DATE,
                            lastChangeDate DATETIME,
                            supplierArticle VARCHAR(255) CHARACTER SET utf8,
                            techSize VARCHAR(255) CHARACTER SET utf8,
                            barcode VARCHAR(255) CHARACTER SET utf8,
                            totalPrice FLOAT,
                            discountPercent INT,
                            warehouseName VARCHAR(255) CHARACTER SET utf8,
                            countryName VARCHAR(255) CHARACTER SET utf8,
                            oblastOkrugName VARCHAR(255) CHARACTER SET utf8,
                            regionName VARCHAR(255) CHARACTER SET utf8,
                            incomeID INT,
                            odid INT,
                            spp INT,
                            forPay FLOAT,
                            finishedPrice FLOAT,
                            priceWithDisc FLOAT,
                            nmId INT,
                            subject VARCHAR(255) CHARACTER SET utf8,
                            category VARCHAR(255) CHARACTER SET utf8,
                            brand VARCHAR(255) CHARACTER SET utf8,
                            gNumber VARCHAR(255) CHARACTER SET utf8,
                            sticker VARCHAR(255) CHARACTER SET utf8,
                            srid VARCHAR(255) CHARACTER SET utf8,
                            saleID_status VARCHAR(255) CHARACTER SET utf8
                            );'''

    # Создаем таблицу в базе данных
    cur.execute(create_table_query)

    # Определяем запрос для добавления данных в таблицу
    insert_data_query = '''INSERT INTO sales   (date,
                                                lastChangeDate,
                                                supplierArticle,
                                                techSize,
                                                barcode,
                                                totalPrice,
                                                discountPercent,
                                                warehouseName,
                                                countryName,
                                                oblastOkrugName,
                                                regionName,
                                                incomeID,
                                                odid,
                                                spp,
                                                forPay,
                                                finishedPrice,
                                                priceWithDisc,
                                                nmId,
                                                subject,
                                                category,
                                                brand,
                                                gNumber,
                                                sticker,
                                                srid,
                                                saleID_status) 
                           VALUES (%s, %s, %s, %s, %s, %s, %s, %s , %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);'''

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
        countryName = df.at[i, 'countryName']
        oblastOkrugName = df.at[i, 'oblastOkrugName']
        regionName = df.at[i, 'regionName']
        incomeID = df.at[i, 'incomeID']
        odid = df.at[i, 'odid']
        spp = df.at[i, 'spp']
        forPay = df.at[i, 'forPay']
        finishedPrice = df.at[i, 'finishedPrice']
        priceWithDisc = df.at[i, 'priceWithDisc']
        nmId = df.at[i, 'nmId']
        subject = df.at[i, 'subject']
        category = df.at[i, 'category']
        brand = df.at[i, 'brand']
        gNumber = df.at[i, 'gNumber']
        sticker = df.at[i, 'sticker']
        srid = df.at[i, 'srid']
        saleID_status = df.at[i, 'saleID_status']

        cur.execute(insert_data_query,
                    (date, lastChangeDate, supplierArticle, techSize, barcode, totalPrice, discountPercent,
                     warehouseName, countryName, oblastOkrugName, regionName, incomeID, odid, spp, forPay,
                     finishedPrice, priceWithDisc, nmId, subject, category, brand, gNumber, sticker, srid, saleID_status))

    # Сохраняем изменения в базе данных
    con.commit()

# Закрываем соединение с базой данных
con.close()
