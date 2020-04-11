"""
MySQL4 - Simple to use pymysql wrappers. An understanding of SQL statements is preferred.

4.0     DatabaseConnection class created.
        RawQuery class created.
        TableQuest class created.
"""

import pymysql
import traceback

__author__ = 'CplBDJ'
__version__ = '4.0a'


username = 'root'
password = ''


class DatabaseConnection:
    """
    Allows a connection to the database server.
    """
    def __init__(self, host='localhost', user=None, passwd=None, port=3306):
        self.host = host
        self.user = user or username
        self.passwd = passwd or password
        self.port = port

    def new(self):
        return pymysql.connect(host=self.host,
                               user=self.user,
                               passwd=self.passwd,
                               port=self.port)

    def query(self, sql:str) -> dict:
        """
        Does the actual sql query. Returns a dict with keys:
        'error'     None or the traceback
        'sql'       The sql passed.
        'data'      The data that has been requested.
        'columns'   The data's column information.
        """
        connection = self.new()
        cursor = connection.cursor()

        error = None
        data = list()
        columns = list()

        try:
            cursor.execute(sql)
            data = cursor.fetchall()
            columns = cursor.description

        except pymysql.err.MySQLError:
            error = traceback.format_exc()

        finally:
            connection.close()

            return dict(error=error,
                        sql=sql,
                        data=data,
                        columns=columns)

    def submit(self, sql: str) -> dict:
        """
        Does the actual sql submission.
        Returns a dict with keys:
        'error'     None or the traceback
        'sql'       The sql passed.
        """
        connection = self.new()
        cursor = connection.cursor()

        error = None

        try:
            cursor.execute(sql)
            connection.commit()
            error = False
        except pymysql.err.MySQLError:
            connection.rollback()
            error = traceback.format_exc()
        finally:
            connection.close()
            return dict(error=error,
                        sql=sql)


class RawQuery:
    """
    A more raw response to the SQL server. This can be used separately but it's purpose is to be subclassed.
    This shows the use of TableQuery, defined elsewhere.

    Allows you to use "with" statement or just call it normally.
    "where" is the SQL WHERE statement.
    "like" is the SQL LIKE statement.
    "sort" is the SQL SORT BY statement.

    The "where" and "like" parameters are structured the same. They can be used interchangablely.

    >>> from pprint import pprint  # For readability

    You can pass the username & password to the class.
    >>> with TableQuery(user='user', passwd='password') as query:
    ...     pprint(query('Apps', 'Users', select='User', where={'User': 'nick'}))
    [{'User': 'nick'}]

    >>> with TableQuery(user='user', passwd='password') as query:
    ...     pprint(query('Apps', 'Users', select='User, Name', where='User like "%k%"'))
    [{'Name': 'Nick', 'User': 'nick'}, {'Name': 'Blake', 'User': 'blake'}]

    You can also set the module's username and password, that way you don't have to pass them.
    >>> username = 'user'
    >>> password = 'password'
    >>> query = TableQuery()
    >>> pprint(query('Apps', 'Users'))
    [{'Initals': None,
      'Name': None,
      'UID': 0,
      'User': 'root'},
     {'Initals': 'NRJ',
      'Name': 'Nick',
      'UID': 1,
      'User': 'nick'},
     {'Initals': 'TJ',
      'Name': 'Tony',
      'UID': 2,
      'User': 'tony'},
     {'Initals': 'JB',
      'Name': 'Jesse',
      'UID': 3,
      'User': 'jesse'},
     {'Initals': 'MM',
      'Name': 'Mighty Mouse',
      'UID': 4,
      'User': 'mightymouse'},
     {'Initals': 'BO',
      'Name': 'Blake',
      'UID': 5,
      'User': 'blake'}]

    >>> pprint(query('Apps', 'Users', like=[('User', '%m%'), ('User', '%n%')]))
    [{'Initals': None,
      'Name': None,
      'UID': 0,
      'User': 'root'},
     {'Initals': 'NRJ',
      'Name': 'Nick',
      'UID': 1,
      'User': 'nick'},
     {'Initals': 'TJ',
      'Name': 'Tony',
      'UID': 2,
      'User': 'tony'},
     {'Initals': 'JB',
      'Name': 'Jesse',
      'UID': 3,
      'User': 'jesse'},
     {'Initals': 'MM',
      'Name': 'Mighty Mouse',
      'UID': 4,
      'User': 'mightymouse'},
     {'Initals': 'BO',
      'Name': 'Blake',
      'UID': 5,
      'User': 'blake'}]

    >>> pprint(query('Apps', 'Users', where='User="tony" or User="root"', sort='Name'))
    [{'Initals': None,
      'Name': None,
      'UID': 0,
      'User': 'root'},
     {'Initals': 'TJ',
      'Name': 'Tony',
      'UID': 2,
      'User': 'tony'}]

    >>> pprint(query('Apps', 'Users', sql='WHERE User="tony" or User="root" ORDER BY `Name`'))
    [{'Initals': None,
      'Name': None,
      'UID': 0,
      'User': 'root'},
     {'Initals': 'TJ',
      'Name': 'Tony',
      'UID': 2,
      'User': 'tony'}]
    # Errors can be accessed using the 'errors' method. It will only show the last query's error.
    >>> pprint(query('Show', 'error'))
    []

    >>> pprint(query.error)
    ('Traceback (most recent call last):\n'
     '  File "/home/nick/Scripts/python3/MySQL4/MySQL4.py", line 52, in query\n'
     '    cursor.execute(sql)\n'
     '  File '
     '"/home/nick/Scripts/python3/MySQL4/lib/python3.8/site-packages/pymysql/cursors.py", '
     'line 170, in execute\n'
     '    result = self._query(query)\n'
     '  File '
     '"/home/nick/Scripts/python3/MySQL4/lib/python3.8/site-packages/pymysql/cursors.py", '
     'line 328, in _query\n'
     '    conn.query(q)\n'
     '  File '
     '"/home/nick/Scripts/python3/MySQL4/lib/python3.8/site-packages/pymysql/connections.py", '
     'line 517, in query\n'
     '    self._affected_rows = self._read_query_result(unbuffered=unbuffered)\n'
     '  File '
     '"/home/nick/Scripts/python3/MySQL4/lib/python3.8/site-packages/pymysql/connections.py", '
     'line 732, in _read_query_result\n'
     '    result.read()\n'
     '  File '
     '"/home/nick/Scripts/python3/MySQL4/lib/python3.8/site-packages/pymysql/connections.py", '
     'line 1075, in read\n'
     '    first_packet = self.connection._read_packet()\n'
     '  File '
     '"/home/nick/Scripts/python3/MySQL4/lib/python3.8/site-packages/pymysql/connections.py", '
     'line 684, in _read_packet\n'
     '    packet.check_error()\n'
     '  File '
     '"/home/nick/Scripts/python3/MySQL4/lib/python3.8/site-packages/pymysql/protocol.py", '
     'line 220, in check_error\n'
     '    err.raise_mysql_exception(self._data)\n'
     '  File '
     '"/home/nick/Scripts/python3/MySQL4/lib/python3.8/site-packages/pymysql/err.py", '
     'line 109, in raise_mysql_exception\n'
     '    raise errorclass(errno, errval)\n'
     'pymysql.err.ProgrammingError: (1146, "Table \'Show.error\' doesn\'t '
     'exist")\n',
     'SELECT * FROM `Show`.`error` None')

    """
    def __init__(self, host='localhost', user=None, passwd=None, port=3306):
        self._connection = DatabaseConnection(host, user, passwd, port)
        self.error = None, None

    def __call__(self, database: str, table: str, select:str = '*',
                 where: str or dict or list = None, # where x = "y"
                 like: str or dict or list = None,  # where x like "y"
                 sort: str = None,
                 sql: str = None):
        self.error = None, None

        _sql = list()

        if isinstance(where, dict):
            spam = [f'`{key}`="{where[key]}"' for key in where]
            _sql.append(f'WHERE {" and ".join(spam)}')
        elif isinstance(where, list):
            spam = [f'`{item[0]}`="{item[1]}"' for item in where]
        elif where and isinstance(where, str):
            _sql.append(f'WHERE {where}')

        if where and like:
            _sql.append(' and ')

        if isinstance(like, dict):
            spam = [f'`{key}`="{like[key]}"' for key in like]
            _sql.append(f'WHERE {" and ".join(spam)}')
        elif isinstance(like, list):
            spam = [f'`{item[0]}`="{item[1]}"' for item in like]
        elif where and isinstance(like, str):
            _sql.append(f'WHERE {like}')

        if sort:
            _sql.append(f'ORDER BY `{sort}`')

        if _sql:
            sql = ' '.join(_sql)

        return self._parse(self._connection.query(f'SELECT {select} FROM `{database}`.`{table}` {sql}'))

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        return None

    def _parse(self, response):
        """
        Subclasses should override this and return the data as requested.
        """
        return response


class TableQuery(RawQuery):
    """
    Subclasses RawQuery, check RawQuery for usage examples. Returns the table as a dict.
    """
    def _parse(self, response):
        """
        Overrides the parent classes method.
        Returns the table data as a dict.
        """
        if response['error']:
            self.error = response['error'], response['sql']
            return tuple()

        keys = tuple(key[0] for key in response['columns'])
        return [dict(zip(keys, line)) for line in response['data']]
