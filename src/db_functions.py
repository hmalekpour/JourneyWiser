import psycopg2


def db_insert(db_name, user_name, host, port, command):
    """
    method used to establish connection and give an insertion command to database.
    ...

    Parameters:
    -----------
    db_name : str
        name of data base
    user_name : str
        database user name
    host : str
        database host address
    port : int
        port number to be used for database connection
    command : str
        the insertion command in sql

    Returns:
    --------
    None
    """
    #DB connection
    conn = psycopg2.connect(database = db_name, user = user_name, host = host, port = port)

    # create a psycopg2 cursor that can execute queries
    cursor = conn.cursor()

    # Insert data to table
    cursor.execute(command)
    conn.commit() # <--- makes sure the change is shown in the database

    #close curser
    cursor.close()
    conn.close()


def db_get(db_name, user_name, host, port, command):
    """
    method used to establish a database connection, give an query command and return the results.
    ...

    Parameters:
    -----------
    db_name : str
        name of data base
    user_name : str
        database user name
    host : str
        database host address
    port : int
        port number to be used for database connection
    command : str
        database querry in sql

    Returns:
    --------
    rows : list
        rows queried from database as a list of touples, empty list if none found.
    """
        #DB connection
        conn = psycopg2.connect(database = db_name, user = user_name, host = host, port = port)

        # create a psycopg2 cursor that can execute queries
        cursor = conn.cursor()

        # Insert data to table
        cursor.execute(command)
        rows = db.fetchal()

        #close curser
        cursor.close()
        conn.close()

        return (rows)
