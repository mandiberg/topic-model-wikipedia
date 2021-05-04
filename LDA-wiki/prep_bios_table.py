import json
import mysql.connector
from wikitextparser import remove_markup, parse
import databaseconfig as cfg

mydb = mysql.connector.connect (
    host= cfg.mysql["host"],
    user=cfg.mysql["user"],
    password=cfg.mysql["password"],
    database=cfg.mysql["database"]
)

mycursor = mydb.cursor()
mycursor.execute("CREATE TABLE bios_text (id VARCHAR(20),enwiki_title VARCHAR(500),occupation VARCHAR(200),gender VARCHAR(100),citizenship VARCHAR(200), article longtext)")

selectQuery = "SELECT * FROM bios WHERE article is not NULL"
mycursor.execute(selectQuery)
myresult = mycursor.fetchall()

for result in myresult:
    if result is not None:
        id = result[0]
        title = result[1]
        occupation = result[2]
        gender = result[3]
        citizenship = result[4]
        article = result[5]
        title = title.replace("'","''")
        try:
            plain=remove_markup(article)
            plain = plain.replace("'","''")

            # write to the new table
            mycursor.execute(f"INSERT INTO bios_text(id, enwiki_title, occupation, gender, citizenship, article) VALUES('{id}', '{title}', '{occupation}', '{gender}', '{citizenship}', '{plain}')")
            mydb.commit()


        except:
            mycursor.execute(f"UPDATE bios SET Err = TRUE where id='{id}'")
            mydb.commit()
            print(id)

print("Finished removing wikitext markup.")
