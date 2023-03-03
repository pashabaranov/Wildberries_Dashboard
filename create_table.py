import gspread
from oauth2client.service_account import ServiceAccountCredentials

# Ссылки, к которым нам нужен доступ
scope = ['https://www.googleapis.com/auth/spreadsheets',
         'https://www.googleapis.com/auth/drive']

# Читаем ключи из json (credentials)
credentials = ServiceAccountCredentials.from_json_keyfile_name('gs_credentials.json', scope)

# Авторизация в проектной почте
client = gspread.authorize(credentials)

# Создать таблицу под названием FirstSheet и запушить её в аккаунт с ЛИЧНОЙ почтой
sheet = client.create('FirstSheet')
sheet.share('your.personal.mail@gmail.com', perm_type='user', role='writer')
