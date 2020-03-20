import mysql.connector
from mysql.connector import Error

mySQLConnection = mysql.connector.connect(host='localhost',
                                          database='huwebshop',
                                          user='root',
                                          password='SlacksP@ss41!')
cursor = mySQLConnection.cursor()

#De sql query
#https://stackoverflow.com/questions/28383546/retrieve-all-ids-where-name-like-current-name-in-one-query/28383840#28383840
sql_select_query = """select id,
        (SELECT GROUP_CONCAT(id SEPARATOR ', ')
        FROM products
        WHERE name like %s
        AND name != %s) as lookalikes
        FROM products
        WHERE name = %s"""

def createtable(): #Table aanmaken voor recommendaties
    cursor.execute("DROP TABLE IF EXISTS recommendaties")
    cursor.execute("CREATE TABLE recommendaties (itemid VARCHAR(255) PRIMARY KEY, recommendsforitem1 VARCHAR(255) NOT NULL, recommendsforitem2 VARCHAR(255) NOT NULL, recommendsforitem3 VARCHAR(255) NOT NULL, recommendsforitem4 VARCHAR(255) NOT NULL)")

def desqlding(vergelijking, originelenaam):
    cursor.execute(sql_select_query, (('%' + vergelijking + '%'), originelenaam, originelenaam)) #% ervoor en erna om te zorgen dat er wordt gekeken wat in vergelijking staat, ergens in de productnaam staat. Originele naam is wat het product niet mag zijn en de andere ori naam is dat hij daaronder word opgeslagen
    record = cursor.fetchall()
    return record

def eindlocatie(recommendsvoordit, recomlist):
    mySql_insert_query2 = "INSERT INTO recommendaties (itemid, recommendsforitem1, recommendsforitem2, recommendsforitem3, recommendsforitem4) VALUES (%s, %s, %s, %s, %s)"
    gelimiteerderecommends = [recommendsvoordit]
    teller = 0                              #Om te voorkomen dat er teveel recommendaties zijn
    for item in recomlist:
        gelimiteerderecommends.append(item)
        teller += 1
        if teller == 4:
            break
    gelimiteerderecommends = tuple(gelimiteerderecommends) #recommends als tuple
    cursor.execute(mySql_insert_query2, gelimiteerderecommends) #insert statement
    mySQLConnection.commit()

def recommendatiemaken(originelenaam, vergelijking, recomlist):
    try:                                    #Wanneer een duplicate key inzit, deze wordt opgevangen door de try except
        record = desqlding(vergelijking, originelenaam)
        meerdereprodid = record[0][1]       #Zorgen dat elke product eruit wordt gepakt
        if meerdereprodid == None:          #Er kan niets in meerdereprodid zitten, om errors te voorkomen
            elkeprodid = [0]
        else:                               #Als er wel wat in stond, dan staat het als tuple, wat fout is
            elkeprodid = str(meerdereprodid).split(', ')
        for i in elkeprodid:
            if i not in recomlist and i != 0:
                recomlist.append(i)
        if len(recomlist) < 4:              #Als er geen 4 producten vindbaar zijn met de volledige naam, dan split ik op het eerste woord van de naam. Als het product een merk heeft, dan is de merk vaak het eerste woord.
            if ' ' in vergelijking:
                vergelijking = originelenaam.split(' ')[0]
            else:
                vergelijking = vergelijking [:len(vergelijking)//2] #Als er geen spatie inzit, oftewel maar 1 woord is. Of als er zelfs met alleen het eerste woord geen 4 recommendaties zijn dan wordt het woord doormidden gehakt.
            recommendatiemaken(originelenaam, vergelijking, recomlist)
        else:
            recommendsvoordit = record[0][0]
            eindlocatie(recommendsvoordit, recomlist)

    except mysql.connector.Error as error: #Print uit waar de error zit
        print("Failed to get record from MySQL table: {}".format(error))

def start(originelenaam):
    recomlist = []                          #Lijst van recommendaties voor het product
    originelenaam = ''.join(originelenaam)  #Originelenaam was nog een tuple
    vergelijking = originelenaam            #Zodat ik de originele naam kan aanpassen, zonder de naam te verliezen. Vergelijking is diegene die aangepast wordt
    recommendatiemaken(originelenaam, vergelijking, recomlist)

def beginstart():
    cursor.execute('SELECT name FROM products') #Haalt alle product namen binnen
    resultaat = cursor.fetchall()
    for i in resultaat:                     #Met elk product naam gaat de code werken in start(i), i is een productnaam
        start(i)
    if (mySQLConnection.is_connected()):
        cursor.close()
        mySQLConnection.close()

createtable()                               #Eerst table
beginstart()                                #Dan de code
