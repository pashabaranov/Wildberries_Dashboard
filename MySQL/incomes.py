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
Authorization = 'your_WB_API_key'
url_incomes = 'https://statistics-api.wildberries.ru/api/v1/supplier/incomes'
headers = {'Authorization': Authorization}
param_request = {'dateFrom': '2018-01-01'}
response_incomes = requests.get(url_incomes, headers=headers, params=param_request)
wb_json_incomes = response_incomes.content

# Наши данные
df = pd.read_json(wb_json_incomes)

# Если новых данных нет, то пропускаем
if df.shape[0] == 0:
    pass
else:
    # Переименовываем колонку
    df = df.rename(columns={'incomeId': 'incomeID'})

    # Убираем лишние колонки
    df.drop(columns=['number'], inplace=True)

    # Приводим lastChangeDate к формату даты
    df['lastChangeDate'] = pd.to_datetime(df['lastChangeDate'])

    # Приводим barcode к формату object
    df['barcode'] = df['barcode'].astype('object')

    # Создаем курсор для работы с базой данных
    cur = con.cursor()

    # Удалим все предыдущие значения в таблице incomes
    delete_table_values_query = "DELETE FROM incomes"

    try:
        cur.execute(delete_table_values_query)
        con.commit()
        print("Все записи удалены успешно")
    except:
        con.rollback()
        print("Ошибка удаления записей")

    # Определяем запрос для создания таблицы
    create_table_query = '''CREATE TABLE IF NOT EXISTS incomes (
                            incomeID INT,
                            date DATE,
                            lastChangeDate DATETIME,
                            supplierArticle VARCHAR(255) CHARACTER SET utf8,
                            techSize VARCHAR(255) CHARACTER SET utf8,
                            barcode VARCHAR(255) CHARACTER SET utf8,
                            quantity INT,
                            totalPrice FLOAT,
                            dateClose VARCHAR(255) CHARACTER SET utf8,
                            warehouseName VARCHAR(255) CHARACTER SET utf8,
                            nmId INT,
                            status VARCHAR(255) CHARACTER SET utf8
                            );'''

    # Создаем таблицу в базе данных
    cur.execute(create_table_query)

    # Определяем запрос для добавления данных в таблицу
    insert_data_query = '''INSERT INTO incomes (incomeID,
                                                date,
                                                lastChangeDate,
                                                supplierArticle,
                                                techSize,
                                                barcode,
                                                quantity,
                                                totalPrice,
                                                dateClose,
                                                warehouseName,
                                                nmId,
                                                status) 
                           VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);'''

    # Перебираем строки датафрейма и добавляем их в базу данных
    for i in df.index:
        incomeID = df.at[i, 'incomeID']
        date = df.at[i, 'date'].strftime('%Y-%m-%d')
        lastChangeDate = df.at[i, 'lastChangeDate'].strftime('%Y-%m-%d %H:%M:%S')
        supplierArticle = df.at[i, 'supplierArticle']
        techSize = df.at[i, 'techSize']
        barcode = df.at[i, 'barcode']
        quantity = df.at[i, 'quantity']
        totalPrice = df.at[i, 'totalPrice']
        dateClose = df.at[i, 'dateClose']
        warehouseName = df.at[i, 'warehouseName']
        nmId = df.at[i, 'nmId']
        status = df.at[i, 'status']

        cur.execute(insert_data_query,
                    (incomeID, date, lastChangeDate, supplierArticle, techSize, barcode, quantity, totalPrice,
                     dateClose, warehouseName, nmId, status))

    # Сохраняем изменения в базе данных
    con.commit()

# Закрываем соединение с базой данных
con.close()
