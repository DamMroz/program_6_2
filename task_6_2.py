import csv
import sqlite3
from sqlite3 import Error


def create_connection(db_file):
    connection = None
    try:
        connection = sqlite3.connect(db_file)
        return connection
    except Error as error:
        print(error)
    return connection


def execute_sql_commands(connection, sql):
    try:
        c = connection.cursor()
        c.execute(sql)
    except Error as error:
        print(error)


def add_station(connection, station_data, measure_data):
    sql_insert = '''INSERT INTO stations (station,latitude,longitude,elevation,name,country,state)
                    VALUES(?,?,?,?,?,?,?)'''
    sql_insert_2 = '''INSERT INTO stations (station,date,precib,tobs)
                    VALUES(?,?,?,?)'''
    cur = connection.cursor()
    for x in range(1,len(station_data)):
        cur.execute(sql_insert, station_data[x])
        connection.commit()
    for y in range(1,len(measure_data)):
        cur.execute(sql_insert_2, measure_data[y])
        connection.commit()


if __name__ == "__main__":

    create_stations_sql = """
        CREATE TABLE IF NOT EXISTS stations (
            id integer PRIMARY KEY,
            station text VARCHAR(255),
            latitude float VARCHAR(255),
            longitude float VARCHAR(255),
            elevation float VARCHAR(255),
            name text VARCHAR(255),
            country text VARCHAR(255),
            state text VARCHAR(255),
            date text VARCHAR(255),
            precib float VARCHAR(255),
            tobs integer VARCHAR(255)
        ); """


    db_file="station.db" 


    with open('clean_measure.csv', newline='') as f:
        reader = csv.reader(f)
        measure_data = [tuple(row) for row in reader]


    with open('clean_stations.csv', newline='') as g:
        reader = csv.reader(g)
        station_data = [tuple(row) for row in reader]

    connection = create_connection(db_file)
    if connection is not None:
        execute_sql_commands(connection, create_stations_sql)

    add_station(connection,station_data,measure_data)

    print(connection.execute("SELECT * FROM stations LIMIT 5").fetchall())

    connection.close()