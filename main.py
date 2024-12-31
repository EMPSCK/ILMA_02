import config
import pymysql


male = open('male_names_rus.txt', "r", encoding="utf_8_sig")
female = open('female_names_rus.txt', "r", encoding="utf_8_sig")
male = male.readlines()
female = female.readlines()
for i in range(len(male)):
    male[i] = male[i][0:-1]

male = list(set(male))


for i in range(len(female)):
    female[i] = female[i][0:-1]

female = list(set(female))
conn = pymysql.connect(
            host=config.host,
            port=3306,
            user=config.user,
            password=config.password,
            database=config.db_name,
            cursorclass=pymysql.cursors.DictCursor
        )
with conn:
    cur = conn.cursor()
    for girlName in female:
        sql = "INSERT INTO gender_encoder (`firstName`, `gender`) VALUES (%s, %s)"
        cur.execute(sql, (girlName, 0))
        conn.commit()

    for boyName in male:
        sql = "INSERT INTO gender_encoder (`firstName`, `gender`) VALUES (%s, %s)"
        cur.execute(sql, (boyName, 1))
        conn.commit()
    pass
