###################################################################################################
# SUMMARY
# 1. Connect to a temporary db to get a cursor that can be used to execute queries
# 2. Get the cursor and REMEMBER set automatic commit instead call conn.commit() after each command
# 3. Use cur.execute(<SQL command>) for each SQL command
# 4. Should have a validate code to confirm the commands were executed successfully
# 5. Remember close cursor and connector
###################################################################################################

import psycopg2


# Create a connection to the database
try: 
    conn = psycopg2.connect("host=127.0.0.1 dbname=studentdb user=student password=student")
except psycopg2.Error as e: 
    print("Error: Could not make connection to the Postgres database")
    print(e)


# Use the connection to get a cursor that can be used to execute queries
try: 
    cur = conn.cursor()
except psycopg2.Error as e: 
    print("Error: Could not get curser to the Database")
    print(e)


# Set automatic commit to be true so that each action is committed without having to call conn.commit() after each command
conn.set_session(autocommit = True)


# Create a database to do the work in
try: 
    cur.execute("create database music_library")
except psycopg2.Error as e:
    print(e)


# Add the database name in the connect statement. Let's close our connection to the default database, reconnect to the Udacity database, and get a new cursor
try: 
    conn.close()
except psycopg2.Error as e:
    print(e)
    
try: 
    conn = psycopg2.connect("host=127.0.0.1 dbname=music_library user=student password=student")
except psycopg2.Error as e: 
    print("Error: Could not make connection to the Postgres database")
    print(e)
    
try: 
    cur = conn.cursor()
except psycopg2.Error as e: 
    print("Error: Could not get curser to the Database")
    print(e)

conn.set_session(autocommit=True)


# Create a Song Library that contains a list of songs, including the song name, artist name, year, album it was from, and if it was a single
try: 
    cur.execute("CREATE TABLE IF NOT EXISTS song_library (\
                song_title varchar(255),\
                artist_name varchar(255), \
                year int, \
                album_name varchar(255), \
                single Boolean\
               );\
    ")
except psycopg2.Error as e: 
    print("Error: Issue creating table")
    print (e)


# Insert the following two rows in the table
try: 
    cur.execute("INSERT INTO song_library (song_title, artist_name, year, album_name, single) \
                 VALUES ( 'Across The Universe', 'The Beatles', '1970', 'Let It Be', 'False')"
                )
except psycopg2.Error as e: 
    print("Error: Inserting Rows")
    print (e)
    
try: 
    cur.execute("INSERT INTO song_library (song_title, artist_name, year, album_name, single) \
                 VALUES ( 'Think For Yourself', 'The Beatles', '1965', 'Rubber Soul', 'False')"
                )
except psycopg2.Error as e: 
    print("Error: Inserting Rows")
    print (e)


# Validate your data was inserted into the table
try: 
    cur.execute("SELECT * FROM song_library;")
except psycopg2.Error as e: 
    print("Error: select *")
    print (e)

row = cur.fetchone()
while row:
   print(row)
   row = cur.fetchone()


# And finally close your cursor and connection
cur.close()
conn.close()
