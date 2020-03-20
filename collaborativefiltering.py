import mysql.connector
from mysql.connector import Error

mySQLConnection = mysql.connector.connect(host='localhost',
                                          database='...',           #Database naam
                                          user='root',
                                          password='...')           #Vul je wachtwoord in
cursor = mySQLConnection.cursor()


def createcollabtable():
    global limit                                                            #Globale limiet, de code kan elk getal aan. Pas hieronder het getal aan als je alles wilt doen. Dit was alleen voor snelheid tijdens tests
    limit = 500
    cursor.execute("DROP TABLE IF EXISTS profielgezien")                    #Aanmaak en droppen van alle tabellen
    cursor.execute("CREATE TABLE profielgezien (profid VARCHAR(255) PRIMARY KEY, kindofcust VARCHAR(255), recommendsforitem1 VARCHAR(255) NULL, recommendsforitem2 VARCHAR(255) NULL, recommendsforitem3 VARCHAR(255) NULL, recommendsforitem4 VARCHAR(255) NULL, recommendsforitem5 VARCHAR(255) NULL, recommendsforitem6 VARCHAR(255) NULL, recommendsforitem7 VARCHAR(255) NULL, recommendsforitem8 VARCHAR(255) NULL, recommendsforitem9 VARCHAR(255) NULL, recommendsforitem10 VARCHAR(255) NULL)")
    cursor.execute("DROP TABLE IF EXISTS colrecommendaties")
    cursor.execute("CREATE TABLE colrecommendaties (profid VARCHAR(255) PRIMARY KEY, recommendsforitem1 VARCHAR(255) NOT NULL, recommendsforitem2 VARCHAR(255) NOT NULL, recommendsforitem3 VARCHAR(255) NOT NULL, recommendsforitem4 VARCHAR(255) NOT NULL)")

def resultaatlooper(resultaatproduct, productlijst):                    #Hier checked de code de resultaten in een looper
    for j in resultaatproduct:
        product = ''.join(j)                                            #j is een product uit resultaatproduct, oftewel een productid, alleen is het nog een tuple.
        if product not in productlijst:                                 #product is een productid die is gekocht, hiermee kijkt de code naar soortgelijken mensen, maar zolang hij niet al in de lijst staat.
            productlijst.append(product)
        if len(productlijst) == 11:
            break
    if len(productlijst) > 3:                                           #productlijst is hoeveel producten dit persoon heeft bekeken, alleen zitten hier profielid en segment al in. Waardoor het limiet op 3 moet zitten.
        while len(productlijst) != 12:                                  #Het limiet zit op 3 om te zorgen dat mensen die 2 producten hebben bekeken alleen inzitten. Dit zou kunnen worden verlaagd naar 1 product als de scope groter is dan 500.
            productlijst.append("null")                                 #om te zorgen dat alles ingevuld wordt, ook al maak ik het nullable. Om crashes te voorkomen vul ik de overige aan met null
        mySql_insert_query3 = "INSERT INTO profielgezien (profid, kindofcust, recommendsforitem1, recommendsforitem2, recommendsforitem3, recommendsforitem4, recommendsforitem5, recommendsforitem6, recommendsforitem7, recommendsforitem8, recommendsforitem9, recommendsforitem10) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        cursor.execute(mySql_insert_query3, (tuple(productlijst)))      #Liefst wou ik alle MySQL queries buiten de main code halen, als dat geen problemen zou leveren was dat gebeurt. Echter een hele hoop errors later werkt dat niet.
        mySQLConnection.commit()

def profielbekijker(i):
    sql_selecter = ("SELECT prodid FROM profiles_previously_viewed WHERE profid = %s LIMIT %s")
    profielid = [i[1], limit]                                           #i[1] is iemands profielid, limiet blijft het limiet van bovenaan.
    cursor.execute(sql_selecter, (profielid))
    resultaatproduct = cursor.fetchall()                                #Alle productids die dat profiel i[1] heeft bekeken
    productlijst = [i[1], i[0]]
    if resultaatproduct != []:                                          #Als persoon geen producten heeft bekeken, dan gaat de code door naar het volgende persoon. Anders gaat de code naar resultaatlooper om de resultaten te gebruiken.
        resultaatlooper(resultaatproduct, productlijst)

def nogmeerrec(nieuwequery, run, i, recommendaties):                    #Nogmeerrec kijkt alleen naar prodids en zoekt naar andere mensen, andere mensen die ook dat product hebben bekeken en naar wat hun hebben bekeken.
    mySql_query = "SELECT recommendsforitem1, recommendsforitem2, recommendsforitem3, recommendsforitem4, recommendsforitem5, recommendsforitem6, recommendsforitem7, recommendsforitem8, recommendsforitem9, recommendsforitem10 FROM profielgezien WHERE %s IN (recommendsforitem1, recommendsforitem2, recommendsforitem3, recommendsforitem4, recommendsforitem5, recommendsforitem6, recommendsforitem7, recommendsforitem8, recommendsforitem9, recommendsforitem10) OR %s in(recommendsforitem1, recommendsforitem2, recommendsforitem3, recommendsforitem4, recommendsforitem5, recommendsforitem6, recommendsforitem7, recommendsforitem8, recommendsforitem9, recommendsforitem10)"
    cursor.execute(mySql_query, (nieuwequery))
    replyervan = cursor.fetchall()                                      #Alle recente producten die recent bij elkaar zijn bekeken.
    run += 1                                                            #Om te voorkomen dat de code een loop maakt wanneer er een persoon is die 2 producten heeft gekocht waar niemand tot bijna niemand ook eentje van heeft gekocht.
    recommendatiesmaken(replyervan, run, i, recommendaties)

def recommendatiesmaken(replyervan, run, i, recommendaties):            #Hier maken we de recommendaties aan voor een persoon, gebaseerd op alle informatie uit nogmeerrec en collabrec.
    for item in replyervan:
        for itemwithinitem in item:                                     #De reply levert meerdere lists aan, dus moet ik de informatie uit de list met lists halen, waardoor ik hierdoor door elk item heenga
            if itemwithinitem != i[2] and itemwithinitem != i[3] and itemwithinitem != 'null': #Voorkomen dat iemand recommendaties krijgt die ie al heeft bekeken of niet bestaan
                recommendaties.append(itemwithinitem)
            if len(recommendaties) == 4:                                #Voorkomen dat er te veel recommendaties zijn.
                break
    if len(recommendaties) != 4:                                        #Bij te weinig informatie gaat de code verder zoeken
        if run == 0:
            nieuwequery = tuple([i[2], i[3]])
            nogmeerrec(nieuwequery, run, i, recommendaties)             #Als collabrec te weinig recommendaties kan maken, dan gaan we door naar nogmeerrec om het aan te vullen
    else:                                                               #Bij genoeg recommendaties slaat de code het op onder je profielid met 4 productids erachter
        mySql_insert_query3 = "INSERT INTO colrecommendaties (profid, recommendsforitem1, recommendsforitem2, recommendsforitem3, recommendsforitem4) VALUES (%s, %s, %s, %s, %s)"
        recommendaties.insert(0, i[0])
        cursor.execute(mySql_insert_query3, (tuple(recommendaties)))
        mySQLConnection.commit()


def collabrec():                                                        #Beetje hetzelfde als nogmeerrec, alleen kijken we hier naar dat de segments hetzelfde zijn om het gedrag erbij te halen.
    cursor.execute("SELECT profid, kindofcust, recommendsforitem1, recommendsforitem2 FROM profielgezien")
    colabres = cursor.fetchall()
    for i in colabres:
        querylist = list([i[1], i[2], i[3]])                            #segment (wat voor klant het persoon is), een item die het persoon ooit heeft bekeken en nog een item die het persoon heeft bekeken
        mySql_query = "SELECT recommendsforitem1, recommendsforitem2, recommendsforitem3, recommendsforitem4, recommendsforitem5, recommendsforitem6, recommendsforitem7, recommendsforitem8, recommendsforitem9, recommendsforitem10 FROM profielgezien WHERE kindofcust = %s AND %s IN (recommendsforitem1, recommendsforitem2, recommendsforitem3, recommendsforitem4, recommendsforitem5, recommendsforitem6, recommendsforitem7, recommendsforitem8, recommendsforitem9, recommendsforitem10) OR %s in(recommendsforitem1, recommendsforitem2, recommendsforitem3, recommendsforitem4, recommendsforitem5, recommendsforitem6, recommendsforitem7, recommendsforitem8, recommendsforitem9, recommendsforitem10)"
        cursor.execute(mySql_query, (tuple(querylist)))
        replyervan = cursor.fetchall()
        recommendaties = [] #Waar alle recommendaties ingaan
        run = 0 #infinite loop voorkomen
        recommendatiesmaken(replyervan, run, i, recommendaties)

def startensegment():                                                   #De start voor segment en producten te selecteren
    gebruikteids = []
    cursor.execute("SELECT segment, profid FROM sessions WHERE segment NOT IN ('Bouncer', 'Leaver') LIMIT %s", (limit,)) #Een persoon die vaker terug komt zal naast een bouncer en / of een leaver ook wat anders zijn.
    resultaatseg = cursor.fetchall()
    for i in resultaatseg:
        if i[0] == ('',) or i[0] == ('') or i[1] in gebruikteids:       #Niet hetzelfde persoon of een niet bestaand profielid
            continue
        else:
            gebruikteids.append(i[1])                                   #Profielid die al gebruikt zijn
            profielbekijker(i)                                          #Waar we alle informatie gaan bekijken per profiel
    if (mySQLConnection.is_connected()):                                #Om de MySQL goed af te sluiten naderhand
        cursor.close()
        mySQLConnection.close()

createcollabtable()                                                     #Eerst de table in de database aanmaken
startensegment()                                                        #In de eerste table alles stoppen wat een persoon heeft bekeken en wat voor klant het is
collabrec()                                                             #Hierna gaat de tweede table aan de gang om te kijken wat dat persoon heeft bekeken en wat soortgelijke mensen hebben bekeken.