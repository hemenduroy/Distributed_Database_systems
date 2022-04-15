#
# Assignment2 Interface
#

import psycopg2
import os
import sys
from threading import Thread as T

def del_tab(table, cur):
    cur.execute('DROP TABLE IF EXISTS ' + table)

def ptFrag(cur, hmax, frag_width, minlat):
# fragmenting the dataset
    cur.execute(
        'CREATE TABLE pt1 AS select * from points where latitude <= ' + str(minlat + frag_width + hmax))
    cur.execute(
        'CREATE TABLE pt2 AS select * from points where latitude >= ' + str(minlat + frag_width - hmax)
        + ' and ' + ' latitude <= ' + str(minlat + (2 * frag_width) + hmax))
    cur.execute(
        'CREATE TABLE pt3 AS select * from points where latitude >= ' + str(minlat + (2 * frag_width) - hmax)
        + ' and ' + ' latitude <= ' + str(minlat + (3 * frag_width) + hmax))
    cur.execute(
        'CREATE TABLE pt4 AS select * from points where latitude >= ' + str(minlat + (3 * frag_width) - hmax))

def rectFrag(cur, hmax, frag_width, minlat):
    cur.execute(
        'CREATE TABLE rec1 AS select * from rectangles where latitude2 <= ' + str(minlat + frag_width + hmax))
    cur.execute(
        'CREATE TABLE rec2 AS select * from rectangles where latitude1 >= ' + str(minlat + frag_width - hmax)
        + ' and ' + ' latitude2 <= ' + str(minlat + (2 * frag_width) + hmax))
    cur.execute(
        'CREATE TABLE rec3 AS select * from rectangles where latitude1 >= ' + str(minlat + (2 * frag_width) - hmax)
        + ' and ' + ' latitude2 <= ' + str(minlat + (3 * frag_width) + hmax))
    cur.execute(
        'CREATE TABLE rec4 AS select * from rectangles where latitude1 >= ' + str(minlat + (3 * frag_width) - hmax))
    
def genPart(cur):
    cur.execute('select max(st_ymax(geom) - st_ymin(geom)) from rectangles')
    hmax = cur.fetchone()[0]
    cur.execute('select * from rectangles order by latitude2 desc limit 1')
    highest_latitude = cur.fetchone()[3]
    cur.execute('select * from rectangles order by latitude1 asc limit 1')
    minlat = cur.fetchone()[1]
    frag_width = (highest_latitude - minlat) / 4
    del_tab('output', cur)
    cur.execute("CREATE TABLE output (_count bigint, rectangle geometry)")
    del_tab('pt1', cur)
    del_tab('pt2', cur)
    del_tab('pt3', cur)
    del_tab('pt4', cur)
    del_tab('rec1', cur)
    del_tab('rec2', cur)
    del_tab('rec3', cur)
    del_tab('rec4', cur)
    del_tab('rec4', cur)
    rectFrag(cur, hmax / 2, frag_width, minlat)
    ptFrag(cur, hmax / 2, frag_width, minlat)
   
def merge_frag(pointsTable, rectsTable, cur):
    #https://gis.stackexchange.com/questions/68747/spatial-query-st-contains-postgis
    cur.execute(
        'INSERT INTO output (_count,rectangle) SELECT  count( ' + pointsTable + '.geom) AS count , ' + rectsTable + '.geom  as rectangle FROM ' + rectsTable + ' JOIN ' + pointsTable + ' ON st_contains(' + rectsTable + '.geom,' + pointsTable + '.geom) GROUP BY ' + rectsTable + '.geom order by count asc')
   
def sepThread(cur):
    #multithreading
    #https://stackoverflow.com/questions/20838162/how-does-threadpoolexecutor-map-differ-from-threadpoolexecutor-submit
    #https://superfastpython.com/threadpoolexecutor-map/
    threada = T(target=merge_frag, args=('pt1', 'rec1', cur))
    threada.start()
    threada.join()

    threadb = T(target=merge_frag, args=('pt2', 'rec2', cur))
    threadb.start()
    threadb.join()

    threadc = T(target=merge_frag, args=('pt3', 'rec3', cur))
    threadc.start()
    threadc.join()

    threadd = T(target=merge_frag, args=('pt4', 'rec4', cur))
    threadd.start()
    threadd.join()
   
# Do not close the connection inside this file i.e. do not perform openConnection.close()
def parallelJoin(pointsTable, rectsTable, outputTable, outputPath, openConnection):
    cur = openConnection.cursor()
    genPart(cur)
    sepThread(cur)
    cur.execute("SELECT DISTINCT _count, rectangle from output order by _count asc")
    vals = cur.fetchall()
    #write to output file
    with open(outputPath, 'a') as o:
        for val in vals:
            o.write(str(val[0]) + '\n')
        o.close()
    openConnection.commit()

################### DO NOT CHANGE ANYTHING BELOW THIS #############################

# Donot change this function
def getOpenConnection(user='postgres', password='12345', dbname='dds_assignment2'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")


# Donot change this function
def createDB(dbname='dds_assignment2'):
    """
    We create a DB by connecting to the default user and database of Postgres
    The function first checks if an existing database exists for a given name, else creates it.
    :return:None
    """
    # Connect to the default database
    con = getOpenConnection(dbname='postgres')
    con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = con.cursor()

    # Check if an existing database with the same name exists
    cur.execute('SELECT COUNT(*) FROM pg_catalog.pg_database WHERE datname=\'%s\'' % (dbname,))
    count = cur.fetchone()[0]
    if count == 0:
        cur.execute('CREATE DATABASE %s' % (dbname,))  # Create the database
    else:
        print('A database named {0} already exists'.format(dbname))

    # Clean up
    cur.close()
    con.commit()
    con.close()


# Donot change this function
def deleteTables(tablename, openconnection):
    try:
        cursor = openconnection.cursor()
        if tablename.upper() == 'ALL':
            cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
            tables = cursor.fetchall()
            for table_name in tables:
                cursor.execute('DROP TABLE %s CASCADE' % (table_name[0]))
        else:
            cursor.execute('DROP TABLE %s CASCADE' % (tablename))
        openconnection.commit()
    except psycopg2.DatabaseError as e:
        if openconnection:
            openconnection.rollback()
        print('Error %s' % e)
        sys.exit(1)
    except IOError as e:
        if openconnection:
            openconnection.rollback()
        print('Error %s' % e)
        sys.exit(1)
    finally:
        if cursor:
            cursor.close()
