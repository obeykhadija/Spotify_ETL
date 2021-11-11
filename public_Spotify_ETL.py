import json
from datetime import datetime, time, timedelta
from sqlite3.dbapi2 import Timestamp

import pandas as pd
import requests
import sqlalchemy
from numpy.lib.function_base import append
from sqlalchemy.engine import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql.ddl import CreateTable

USER_ID = ''   # spotify user id
AUTH_TOKEN = '' # token generated from Spotify

def run_spotify_etl():
    if __name__ == '__main__':
        # Set headers according to Spotify's API instructions
        headers = {
            'Accept': 'application/json',
            'Content-Type': 'application/json',
            'Authorization': 'Bearer {token}'.format(token=AUTH_TOKEN)
        }

    # EXTRACT

    today = datetime.now()                                # Gather today's date
    yesterday = today - timedelta(days=1)
    two_days_prior = today - timedelta(days=2)           # Use today's date to determine yesterdays date
    tdp_unix_time = int(two_days_prior.timestamp())*1000 # Convert yesterdays date to unix timestamp

    r = requests.get('https://api.spotify.com/v1/me/player/recently-played?after={time}'.format(time=tdp_unix_time), headers=headers)
    data = r.json()

    # Initilize data stores as empty lists
    song = []
    artist = []
    popularity = []
    time_played =[]
    date_played = []

    # Append song info to data stores
    for song_info in data['items']:
        song.append(song_info['track']['name'])
        artist.append(song_info['track']['album']['artists'][0]['name'])
        popularity.append(song_info['track']['popularity'])
        time_played.append(song_info['played_at'][11:22])
        date_played.append(song_info['played_at'][0:10])


    # TRANSFORM

    # Store data as dictionary
    song_dict = {
        'song':song,
        'artist':artist,
        'popularity':popularity,
        'time_played':time_played,
        'date_played':date_played}

    song_df = pd.DataFrame.from_dict(data=song_dict) # Convert dictrionary to pandas df

    # Function to validate date before we load into database
    def data_validation(df):
        # 1. Check if df is empty
        if len(df) == 0:
            print('No songs downloaded. Finishing Execution')
            return False
        # 2. Ensure primary key is unique
        if df['time_played'].is_unique:
            pass
        else:
            raise Exception('Primary key check is violated')

        # 3. Check for null values
        if df.isna().sum().sum() == 0:
            pass
        else:
            raise Exception('Null values found')

        # 4. Ensure all songs are collected from yesterday
        dates = list(song_df['date_played'])
        yesterday_date = str(yesterday)
        for date in dates:
            if date != yesterday_date[0:10]:
                raise Exception('Not all songs are collected from yesterday')
            else:
                pass
            
        return True

    validation_check = data_validation(song_df)

        # LOAD
    if validation_check == True:
        import ibm_db                           # IBM Db2 API
        from sqlalchemy import create_engine

         # Connect to database
        dsn_hostname = ""
        dsn_uid = "" 
        dsn_pwd = ""
        dsn_driver = "{IBM DB2 ODBC DRIVER}"
        dsn_database = "BLUDB"            
        dsn_port = ""         
        dsn_protocol = 'TCPIP'  
        dsn_security = 'SSL'      

        dsn = (
            "DRIVER={0};"
            "DATABASE={1};"
            "HOSTNAME={2};"
            "PROTOCOL={3};"
            "PORT={4};"
            "UID={5};"
            "PWD={6};"
            "SECURITY={7}").format(dsn_driver, dsn_database, dsn_hostname, dsn_protocol, dsn_port, dsn_uid, dsn_pwd, dsn_security)


        # print(dsn) # Ensure connection string is correct

        try:
            conn = ibm_db.connect(dsn, '', '')
            print('Connected to database')
        except:
            print('Unable to connect: ')
            print (ibm_db.conn_errormsg())

        # SQL Queries
        sql_create_query = '''
            CREATE TABLE IF NOT EXISTS my_recent_tracks(
                song VARCHAR(200),
                artist VARCHAR(200),
                popularity INT,
                time_played VARCHAR(200) NOT NULL,
                date_played VARCHAR(200)
            )
        '''
        sql_insert = '''
            INSERT INTO my_recent_tracks 
            VALUES(?,?,?,?,?)
        '''

        try:
            createTable = ibm_db.exec_immediate(conn, sql_create_query)
            print('Table created')
        except:
            print('Table already exists')

        try:
            tuple_of_tuples = tuple([tuple(x) for x in song_df.values])
            prepared_sql = ibm_db.prepare(conn, sql_insert)
            ibm_db.execute_many(prepared_sql, tuple_of_tuples)
            print('Tracks added!')
        except:
            print('Tracks already uploaded!')

        ibm_db.close(conn)
        print()
        print('Connection to database terminated')
    else:
        raise Exception('Validation check has been violated')