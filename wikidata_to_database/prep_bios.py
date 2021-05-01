import mysql.connector
from os import listdir
import os
from csv import reader
import databaseconfig as cfg
mydb = mysql.connector.connect (
    host= cfg.mysql["host"],
    user=cfg.mysql["user"],
    password=cfg.mysql["password"],
    database=cfg.mysql["database"]
)

mycursor = mydb.cursor()
mycursor.execute("CREATE TABLE bios (id VARCHAR(20),enwiki_title VARCHAR(500),occupation VARCHAR(200),gender VARCHAR(100),citizenship VARCHAR(200)  )")



def find_csv_filenames(path_to_dir, suffix=".csv"):
    filenames = listdir(path_to_dir)
    return [filename for filename in filenames if filename.endswith(suffix)]

filenames = find_csv_filenames(os.getcwd())
filenames.sort(key=os.path.getmtime)
for filename in filenames:
    with open(filename, 'r') as read_obj:
        csv_reader = reader(read_obj)
        header = next(csv_reader)
        for row in csv_reader:
            enwiki_title = row[4]
            qid = row[1]
            occupation = row[5]
            gender = row[6]
            citizenship = row[7]

            enwiki_title=enwiki_title.replace("'","''")
            enwiki_title=enwiki_title.replace(" ","_")
            insertQuery = f"INSERT INTO bios VALUES('{qid}', '{enwiki_title}', '{occupation}', '{gender}', '{citizenship}')"
            # insertQuery = "INSERT INTO bio_csv VALUES('test', 'test', 'test', 'test', 'test')"
            print(insertQuery)

            mycursor.execute(insertQuery)
            mydb.commit()

            # query = f"SELECT * FROM fast_wiki WHERE page_title='{enwiki_title}'"
            # query="SELECT * FROM fast_wiki WHERE page_title='George W. Bush'"
            # mycursor.execute(insertQuery)

            # myresult = mycursor.fetchall()

            # for x in mycursor:
            #     print(x)
            # print(row[4])
    # print(filename)
