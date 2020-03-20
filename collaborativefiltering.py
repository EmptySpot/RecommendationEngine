import mysql.connector
from mysql.connector import Error

mySQLConnection = mysql.connector.connect(host='localhost',
                                          database='huwebshop',
                                          user='root',
                                          password='SlacksP@ss41!')
cursor = mySQLConnection.cursor()


def createcollabtable():
    global limit                #Globale limiet, de code kan elk getal aan. Alleen duurt dat voor elke test run te lang waardoor een limiet handig was.
    limit = (500,)
    cursor.execute("DROP TABLE IF EXISTS profielgezien")
    cursor.execute("CREATE TABLE profielgezien (profid VARCHAR(255) PRIMARY KEY, kindofcust VARCHAR(255), recommendsforitem1 VARCHAR(255) NULL, recommendsforitem2 VARCHAR(255) NULL, recommendsforitem3 VARCHAR(255) NULL, recommendsforitem4 VARCHAR(255) NULL, recommendsforitem5 VARCHAR(255) NULL, recommendsforitem6 VARCHAR(255) NULL, recommendsforitem7 VARCHAR(255) NULL, recommendsforitem8 VARCHAR(255) NULL, recommendsforitem9 VARCHAR(255) NULL, recommendsforitem10 VARCHAR(255) NULL)")
    cursor.execute("DROP TABLE IF EXISTS colrecommendaties")
    cursor.execute("CREATE TABLE colrecommendaties (profid VARCHAR(255) PRIMARY KEY, recommendsforitem1 VARCHAR(255) NOT NULL, recommendsforitem2 VARCHAR(255) NOT NULL, recommendsforitem3 VARCHAR(255) NOT NULL, recommendsforitem4 VARCHAR(255) NOT NULL)")

def resultaatlooper(resultaatproduct, productlijst):
    for j in resultaatproduct:
        product = ''.join(j)
        if product not in productlijst:
            productlijst.append(product)
        if len(productlijst) == 11:
            break
    if len(productlijst) > 3:
        while len(productlijst) != 12:
            productlijst.append("null")
        mySql_insert_query3 = "INSERT INTO profielgezien (profid, kindofcust, recommendsforitem1, recommendsforitem2, recommendsforitem3, recommendsforitem4, recommendsforitem5, recommendsforitem6, recommendsforitem7, recommendsforitem8, recommendsforitem9, recommendsforitem10) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        cursor.execute(mySql_insert_query3, (tuple(productlijst)))
        mySQLConnection.commit()

def profielbekijker(i):
    sql_selecter = ("SELECT prodid FROM profiles_previously_viewed WHERE profid = %s LIMIT %s", limit)
    profielid = (i[1],)
    cursor.execute(sql_selecter,(profielid))
    resultaatproduct = cursor.fetchall()
    productlijst = [i[1], i[0]]
    if resultaatproduct != []:
        resultaatlooper(resultaatproduct, productlijst)

def nogmeerrec(nieuwequery, run, i, recommendaties):
    mySql_query = "SELECT recommendsforitem1, recommendsforitem2 FROM profielgezien WHERE %s IN (recommendsforitem1, recommendsforitem2, recommendsforitem3, recommendsforitem4, recommendsforitem5, recommendsforitem6, recommendsforitem7, recommendsforitem8, recommendsforitem9, recommendsforitem10) OR %s in(recommendsforitem1, recommendsforitem2, recommendsforitem3, recommendsforitem4, recommendsforitem5, recommendsforitem6, recommendsforitem7, recommendsforitem8, recommendsforitem9, recommendsforitem10)"
    cursor.execute(mySql_query, (nieuwequery))
    replyervan = cursor.fetchall()
    run = 1
    recommendatiesmaken(replyervan, run, i, recommendaties)

def recommendatiesmaken(replyervan, run, i, recommendaties):
    for item in replyervan:
        for itemwithinitem in item:
            if itemwithinitem != i[2] and itemwithinitem != i[3] and itemwithinitem != 'null':
                recommendaties.append(itemwithinitem)
            if len(recommendaties) == 4:
                break
    if len(recommendaties) != 4:
        nieuwequery = tuple([i[2], i[3]])
        if run == 0:
            nogmeerrec(nieuwequery, run, i, recommendaties)
    else:
        mySql_insert_query3 = "INSERT INTO colrecommendaties (profid, recommendsforitem1, recommendsforitem2, recommendsforitem3, recommendsforitem4) VALUES (%s, %s, %s, %s, %s)"
        recommendaties.insert(0, i[0])
        cursor.execute(mySql_insert_query3, (tuple(recommendaties)))
        mySQLConnection.commit()


def collabrec():
    cursor.execute("SELECT profid, kindofcust, recommendsforitem1, recommendsforitem2 FROM profielgezien")
    colabres = cursor.fetchall()
    for i in colabres:
        querylist = list([i[1], i[2], i[3]])
        mySql_query = "SELECT recommendsforitem1, recommendsforitem2, recommendsforitem3, recommendsforitem4, recommendsforitem5, recommendsforitem6, recommendsforitem7, recommendsforitem8, recommendsforitem9, recommendsforitem10 FROM profielgezien WHERE kindofcust = %s AND %s IN (recommendsforitem1, recommendsforitem2, recommendsforitem3, recommendsforitem4, recommendsforitem5, recommendsforitem6, recommendsforitem7, recommendsforitem8, recommendsforitem9, recommendsforitem10) OR %s in(recommendsforitem1, recommendsforitem2, recommendsforitem3, recommendsforitem4, recommendsforitem5, recommendsforitem6, recommendsforitem7, recommendsforitem8, recommendsforitem9, recommendsforitem10)"
        cursor.execute(mySql_query, (tuple(querylist)))
        replyervan = cursor.fetchall()
        recommendaties = []
        run = 0
        recommendatiesmaken(replyervan, run, i, recommendaties)

def segment():
    gebruikteids = []
    cursor.execute("SELECT segment, profid FROM sessions WHERE segment NOT IN ('Bouncer', 'Leaver') LIMIT %s", limit)
    resultaatseg = cursor.fetchall()
    for i in resultaatseg:
        if i[0] == ('',) or i[0] == ('') or i[1] in gebruikteids:
            continue
        else:
            gebruikteids.append(i[1])
            profielbekijker(i)
    if (mySQLConnection.is_connected()):
        cursor.close()
        mySQLConnection.close()

createcollabtable()
segment()
collabrec()