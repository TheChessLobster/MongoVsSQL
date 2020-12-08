# Evan Seghers
# A mathematical comparison between SQL and NoSQL using the Pydobc API and the pymongo API
# 11/1/2020

import pymongo
import time as clock
import pyodbc
import matplotlib
from matplotlib import pyplot as plt
import numpy as np

def mongoTests():
    print("Hello from MongoTests")
    client = pymongo.MongoClient()
    mydb = client["sampledatabase"] # you can't define it unless you have created a connection inside it.
    mycollection = mydb["MongoSalesRecords"]

    # Search for 1 specific record and record time
    start = clock.perf_counter()
    result = mycollection.find_one({"Region":"Asia"})
    end = clock.perf_counter()
    mongoFindOneTime = end-start
    print("Time for MongoDB to find one record via API is : " + str(mongoFindOneTime))
    #print(result)

    # Search for multiple records that meet a certain rule
    result = mycollection.count_documents({"Region":"Asia"}) #146,017
    #print("There is this many results returned: " + str(result))
    start = clock.perf_counter()
    mycollection.find({"Region":"Asia"})
    end = clock.perf_counter()
    mongoFindManyTime = end-start
    print("Time for MongoDB to find many records via API is : " + str(mongoFindManyTime))
    #print(newresult)

    # Insert those searched records, into a new test table
    newcollection = mydb["InsertionTest"]
    newcollection.delete_many({}) #delete all records to prevent
    toinsert = mycollection.find({"Region": "Asia"})
    start = clock.perf_counter()
    newcollection.insert_many(toinsert)
    end = clock.perf_counter()
    mongoInsertManyTime = end-start
    print("Time for MongoDB to insert many records via API is: " + str(mongoInsertManyTime))


    #calculate Summation of all of one column in collection 2
    pipe = [{'$group': {'_id': None, 'totalProfit': {'$sum': '$Total Profit'}}}]
    start = clock.perf_counter()
    result = newcollection.aggregate(pipeline = pipe)
    end = clock.perf_counter()
    MongoAggregateTime = end-start
    print("Time for MongoDB to sum Aggregate over a collection is: " + str(MongoAggregateTime))

    #calculate order by of entire collection
    update_query = {"region": "Asia"}
    newvalues = {"$set": {"name": "MadeUpPlace"}}
    start = clock.perf_counter()
    result = newcollection.update_many(update_query,newvalues)
    end = clock.perf_counter()
    MongoUpdateTime = end - start
    print("Time for MongoDB to update a value in a collection is: " + str(MongoUpdateTime))

    # Record time to delete those newly inserted records
    start = clock.perf_counter()
    newcollection.delete_many({})
    end = clock.perf_counter()
    mongoManyDeleteTime = end - start
    print("Time for MongoDB to delete many records via API is " + str(mongoManyDeleteTime))

    #Return mongo results
    return [mongoFindOneTime, mongoFindManyTime, mongoInsertManyTime, MongoAggregateTime,MongoUpdateTime, mongoManyDeleteTime]


def sqlTests():
    print("Hello from SQLTests")
    #define the server name and the databse name
    server = 'DESKTOP-JVN219C'
    database = 'master'
    cnxn = pyodbc.connect('DRIVER= {ODBC Driver 17 for SQL Server};\
                          SERVER=' + server + ';\
                          DATABASE=' + database +';\
                          Trusted_Connection=yes;')
    cursor = cnxn.cursor()

    # Search for 1 specific record and record time
    select_query = "Select TOP 1 * from SalesRecords"
    start = clock.perf_counter()
    cursor.execute(select_query)
    end = clock.perf_counter()
    SQLTimeToFindOne = end-start
    # for row in cursor.fetchall():
    #    print(row)
    print("Time for SQL to find one record via API is: " + str(SQLTimeToFindOne))

    # Search for multiple records that meet a certain rule
    select_query = "SELECT * from SalesRecords where Region = 'Asia'"
    start = clock.perf_counter()
    cursor.execute(select_query)
    end = clock.perf_counter()
    SQLTimeToFindMany = end-start
    print("Time for SQL to find many records via API is: " + str(SQLTimeToFindMany))

    # Insert those searched records, into a new test table
    select_query = "select * From SalesRecords where Region = 'Asia'"
    insert_query = """INSERT INTO InsertedSalesRecords
                     VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)"""
    result = cursor.execute(select_query)
    list = []
    for row in result:
        #print(row)
        list.append(row)
    start = clock.perf_counter()
    for row in list:
        cursor.execute(insert_query,row)
    cursor.commit()
    end = clock.perf_counter()
    SQLTimeToInsertMany = end-start
    print("Time for SQL To insert many records via API is: " + str(SQLTimeToInsertMany))

    #record time needed to aggregate total profit for the whole selection
    aggregation_query = "Select sum([Total Profit]) from insertedSalesRecords"
    start = clock.perf_counter()
    cursor.execute(aggregation_query)
    end = clock.perf_counter()
    SQLTimeToAggregateMany = end-start
    print("Time for SQL to sum Aggregate over a collection: " + str(SQLTimeToAggregateMany))

    #record time needed to aggregate and sort for the whole selection by Profit descending
    update_query = "Update insertedSalesRecords set region = 'MadeUpPlace' where region = 'Asia'"
    start = clock.perf_counter()
    cursor.execute(update_query)
    end = clock.perf_counter()
    SQLUpdateBy = end - start
    print("Time for SQL to update by value over a collection: " + str(SQLUpdateBy))




    #record time to delete all of those records
    delete_query = "Delete from InsertedSalesRecords"
    start = clock.perf_counter()
    cursor.execute(delete_query)
    end = clock.perf_counter()
    cursor.commit()
    SQLTimeToDeleteMany = end-start
    print("Time for SQL to delete many records via API is: " + str(SQLTimeToDeleteMany))

    #return SQL Results
    return [SQLTimeToFindOne,SQLTimeToFindMany,SQLTimeToInsertMany,SQLTimeToAggregateMany,SQLUpdateBy,SQLTimeToDeleteMany]

def Plot(mongoResults,sqlResults):
    print("Hello from plotting zone")
    # Plot find one speed 0
    speed = [mongoResults[0],sqlResults[0]]
    plt.bar(["NoSQL","SQL"],speed)
    plt.xlabel("DB Type")
    plt.ylabel("Speed in seconds")
    plt.title("Single Record Query")
    axes = plt.gca()
    axes.set_ylim([0, .05])
    plt.show()
    # Plot find many speed 1
    speed = [mongoResults[1], sqlResults[1]]
    plt.bar(["NoSQL", "SQL"], speed)
    plt.xlabel("DB Type")
    plt.ylabel("Speed in seconds")
    plt.title("150,000 Record Select Query")
    axes = plt.gca()
    axes.set_ylim([0, .005])
    plt.show()

    # Plot insert many speed 2
    speed = [mongoResults[2], sqlResults[2]]
    plt.bar(["NoSQL", "SQL"], speed)
    plt.xlabel("DB Type")
    plt.ylabel("Speed in seconds")
    plt.title("150,000 Record Insertion")
    axes = plt.gca()
    axes.set_ylim([0, 20])
    plt.show()
    # Plot aggregate many speed 3
    speed = [mongoResults[3], sqlResults[3]]
    plt.bar(["NoSQL", "SQL"], speed)
    plt.xlabel("DB Type")
    plt.ylabel("Speed in seconds")
    plt.title("150,000 Record Aggregation")
    axes = plt.gca()
    axes.set_ylim([0, 2])
    plt.show()

    # Plot update many speed 4
    speed = [mongoResults[4], sqlResults[4]]
    plt.bar(["NoSQL", "SQL"], speed)
    plt.xlabel("DB Type")
    plt.ylabel("Speed in seconds")
    plt.title("150,000 Record Update")
    axes = plt.gca()
    axes.set_ylim([0, 1])
    plt.show()

    # Plot delete many speed 5
    speed = [mongoResults[5], sqlResults[5]]
    plt.bar(["NoSQL", "SQL"], speed)
    plt.xlabel("DB Type")
    plt.ylabel("Speed in seconds")
    plt.title("150,000 Record Deletion")
    axes = plt.gca()
    axes.set_ylim([0, 2])
    plt.show()

    #Create double bar graph for all output
    w = .4
    x= ["FindOne","FindMany","InsertMany","AggMany","UpMany","DeleteMany"]
    mongo = [mongoResults[0],mongoResults[1],mongoResults[2],mongoResults[3],mongoResults[4],mongoResults[5]]
    SQL= [sqlResults[0],sqlResults[1],sqlResults[2],sqlResults[3],sqlResults[4],sqlResults[5]]

    bar1 = np.arange(len(x))
    bar2 = [i + w for i in bar1]

    plt.bar(bar1,SQL,w,label = "SQL")
    plt.bar(bar2,mongo,w,label = "NoSQL")


    plt.xlabel("Query Types")
    plt.ylabel("Time in Seconds")
    plt.title("SQL vs NoSQL Query Performance")
    plt.xticks(bar1,x)
    axes = plt.gca()
    axes.set_ylim([0, 20])
    plt.legend()
    plt.show()



    #Create final fully aggregated Mongo Results
    speed = [mongoResults[0],mongoResults[1],mongoResults[2],mongoResults[3],mongoResults[4],mongoResults[5]]
    plt.bar(["FindOne","FindMany","InsertMany","AggMany","UpMany","DeleteMany"], speed)
    plt.xlabel("Query Type")
    plt.ylabel("Speed in seconds")
    plt.title("NoSQL Stats")
    axes = plt.gca()
    axes.set_ylim([0,20])
    plt.show()


    #Create same plot for SQL results
    speed = [sqlResults[0], sqlResults[1], sqlResults[2], sqlResults[3], sqlResults[4],sqlResults[5]]
    plt.bar(["FindOne", "FindMany", "InsertMany", "AggMany","UpMany", "DeleteMany"], speed)
    plt.xlabel("Query Type")
    plt.ylabel("Speed in seconds")
    plt.title("SQL Stats")
    axes = plt.gca()
    axes.set_ylim([0, 20])
    plt.show()

def main():
    # FindOne,FindMany,InsertMany,AggregateSumMany,UpdateMany, DeleteMany
    mongoresults = mongoTests()
    sqlresults = sqlTests()
    # take results and plot them using Matplot lib
    Plot(mongoresults,sqlresults)
main()
