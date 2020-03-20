import mysql.connector
from mysql.connector import Error

mySQLConnection = mysql.connector.connect(host='localhost',
                                          database='huwebshop',
                                          user='root',
                                          password='SlacksP@ss41!')
cursor = mySQLConnection.cursor()

sql_select_query = """select id,
        (SELECT GROUP_CONCAT(id SEPARATOR ', ')
        FROM products
        WHERE name like %s
        AND name != %s) as lookalikes
        FROM products
        WHERE name = %s"""

def createtable():
    cursor.execute("DROP TABLE IF EXISTS recommendaties")
    cursor.execute("CREATE TABLE recommendaties (itemid VARCHAR(255) PRIMARY KEY, recommendsforitem1 VARCHAR(255) NOT NULL, recommendsforitem2 VARCHAR(255) NOT NULL, recommendsforitem3 VARCHAR(255) NOT NULL, recommendsforitem4 VARCHAR(255) NOT NULL)")

def desqlding(vergelijking, originelenaam):
    cursor.execute(sql_select_query, (('%' + vergelijking + '%'), originelenaam, originelenaam))
    record = cursor.fetchall()
    return record

def eindlocatie(recommendsvoordit, recomlist):
    mySql_insert_query2 = "INSERT INTO recommendaties (itemid, recommendsforitem1, recommendsforitem2, recommendsforitem3, recommendsforitem4) VALUES (%s, %s, %s, %s, %s)"
    gelimiteerderecommends = [recommendsvoordit]
    teller = 0
    for item in recomlist:
        gelimiteerderecommends.append(item)
        teller += 1
        if teller == 4:
            break
    gelimiteerderecommends = tuple(gelimiteerderecommends)
    cursor.execute(mySql_insert_query2, gelimiteerderecommends)
    mySQLConnection.commit()

def getLaptopDetail(originelenaam, vergelijking, recomlist):
    try:
        record = desqlding(vergelijking, originelenaam)
        a = record[0][1]
        if a == None:
            b = [0]
        else:
            b = str(a).split(', ')
        for i in b:
            if i not in recomlist and i != 0:
                recomlist.append(i)
        if len(recomlist) < 4:
            if ' ' in vergelijking:
                vergelijking = originelenaam.split(' ')[0]
            else:
                vergelijking = vergelijking [:len(vergelijking)//2]
            getLaptopDetail(originelenaam, vergelijking, recomlist)
        else:
            recommendsvoordit = record[0][0]
            eindlocatie(recommendsvoordit, recomlist)

    except mysql.connector.Error as error:
        print("Failed to get record from MySQL table: {}".format(error))

def start(originelenaam):
    recomlist = []
    originelenaam = ''.join(originelenaam)
    vergelijking = originelenaam
    getLaptopDetail(originelenaam, vergelijking, recomlist)

def beginstart():
    cursor.execute('SELECT name FROM products')
    resultaat = cursor.fetchall()
    for i in resultaat:
        start(i)
    if (mySQLConnection.is_connected()):
        cursor.close()
        mySQLConnection.close()

createtable()
beginstart()
