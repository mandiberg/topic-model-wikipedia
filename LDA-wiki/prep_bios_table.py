import json
import mysql.connector
import databaseconfig as cfg
from wikitextparser import remove_markup, parse
import re
from bs4 import BeautifulSoup


mydb = mysql.connector.connect (
    host= cfg.mysql["host"],
    user=cfg.mysql["user"],
    password=cfg.mysql["password"],
    database=cfg.mysql["database"]
)

mycursor = mydb.cursor()
mycursor.execute("CREATE TABLE bios_final (id VARCHAR(20),enwiki_title VARCHAR(500),occupation VARCHAR(200),gender VARCHAR(100),citizenship VARCHAR(200), article longtext)")

selectQuery = "SELECT * FROM bios WHERE article is not NULL"
mycursor.execute(selectQuery)
myresult = mycursor.fetchall()

def get_table_values(text):
    table_values = []
    for t in text.tables:
        # print(t)
        t1 = []

        for i in t.data():
            # print(i)

            t1.append('\n'.join(list(filter(None, i))))
        table_values.append('\n'.join(t1))

    return '\n'.join(table_values)
def remove_table(plain_text):
    lines = plain_text.split("\n")
    is_table = False
    # end = False
    tableless = []
    for line in lines:
        # line_f = re.search("^\|.*", line)
        # print(line)
        end = False
        table_begin_reg = re.search("^\{\|", line)
        table_end_reg = re.search("\|\}", line)
        # print(table_end_reg)
        if table_begin_reg:
            is_table = True
            # print(line)
        if table_end_reg:
            # lines.remove(line)
            end = True
            # print(line)
            is_table = False
        if not is_table:
            if end:
                continue
            else:
                tableless.append(line)
            # print(line)
            # lines.remove(line)
    return '\n'.join(tableless)
def parse_article(article):
    parsed_wiki = parse(article)
    t_values = get_table_values(parsed_wiki)

    if(len(t_values)>0):
        p_text=parsed_wiki.plain_text()
        p_text = p_text + t_values
        text = remove_table(p_text)
        try:
            soup = BeautifulSoup(text,features="html.parser")
            final = soup.get_text()
        except:
            final = text

    else:
        try:
            soup = BeautifulSoup(parsed_wiki.plain_text(),features="html.parser")
            final = soup.get_text()
        except:
            try:
                final = parsed_wiki.plain_text()
            except:
                final = "ERROR"

    final = final.replace("'","''")
    insertQuery = f"INSERT INTO bios_final(id, enwiki_title, occupation, gender, citizenship, article) VALUES('{id}', '{title}', '{occupation}', '{gender}', '{citizenship}', '{final}')"
    return insertQuery

for result in myresult:
    if result[5] is not None:
        id = result[0]
        title = result[1]
        occupation = result[2]
        gender = result[3]
        citizenship = result[4]
        article = result[5]
        title = title.replace("'","''")



        try:
            qr = parse_article(article)
            # write to the new table
            mycursor.execute(qr)
            mydb.commit()


        except:
            mycursor.execute(f"UPDATE bios SET Err = TRUE where id='{id}'")
            mydb.commit()
            print(id)

print("Finished removing wikitext markup.")
