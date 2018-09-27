import sqlite3

class DBSQLITE(object):
    connection =None;
    def __enter__(self , database='./db/database.db'):
        """Initialize a new or connect to an existing database.

        Accept setup statements to be executed.
        """
        self.DATABASE = database

        #Connect to DB at launch
        self.connect()

        return self

    def connect(self):
        if self.connection is None:
            self.connection  = sqlite3.connect(self.DATABASE)
            self.connection.row_factory = sqlite3.Row
            self.cursor = self.connection.cursor()
            self.connected = True
            print("Opened database successfully");
    def close(self):
        if self.connection is not None:
            self.connection.commit()
            self.connection.close()
            self.connected = False
    def init_db(self):
        with open('schema.sql', mode='r') as f:
            self.cursor.executescript(f.read())
        self.connection.commit()
    def query_db(self,query, args=(), one=False):
        cur = self.connection.execute(query, args)
        rv = cur.fetchall()
        cur.close()
        return rv if rv else None
    def logUserInfo(self, args = ()):
        return self.query_db('INSERT INTO askLegalTrackerTable(user, nodes, entity,where_clause,created_date)  VALUES (?, ?, ?, ?, ?)', args=args)
    def getUserInfo(self, args = ()):
        return self.query_db('SELECT nodes, entity, where_clause FROM askLegalTrackerTable')
    def __exit__(self, type, value, traceback):
        self.close()
