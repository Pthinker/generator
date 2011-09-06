import PySQLPool
from metric.conf import ConfigReader
from metric.aes_coder import AESCoder
import MySQLdb

class DBManager:
    __connected = None
    __connection = None
    __query = None
    __connections = {}
    __config = None
    __aescoder = None

    def __init__(self):
        if not DBManager.__connected:
            self._init()

    @classmethod
    def _init(cls):
        cls.__config = ConfigReader()
        cls.__aescoder = AESCoder()
        cls.__connect_to_main_db()
        cls.__connected = True

    @classmethod
    def __connect_to_main_db(cls):
        cls.__connection, cls.__query = cls._connect_to_db(cls.__config.db, cls.__config.db_user, cls.__config.db_passwd)

    @classmethod
    def _connect_to_db(cls, db_name, db_user, db_passwd):
        try:
            connection = PySQLPool.getNewConnection(
                                                    host=cls.__config.db_host,
                                                    username=db_user,
                                                    password=cls.__aescoder.decrypt(db_passwd),
                                                    db=db_name,
                                                    commitOnEnd=True,
                                                    use_unicode=True,
                                                    charset = 'utf8')
        except MySQLdb.Error, e:
            raise Exception("connection %d: %s" % (e.args[0], e.args[1]))
        except Exception, exc:
            raise Exception("cannot connect to db - %s" % exc)

        try:
            query = PySQLPool.getNewQuery(connection)
        except MySQLdb.Error, e:
            raise Exception("connection %d: %s" % (e.args[0], e.args[1]))

        return connection, query

    @classmethod
    def get_query(cls):
        if not cls.__connected:
            cls._init()
            cls.__connect_to_main_db()
        if cls.__query:
            return cls.__query
        else:
            raise Exception("local db connection lost")

    @classmethod
    def get_db_query(cls, db):
        if not cls.__connected:
            #cls.__init__(cls)
            cls._init()
            cls.__connect_to_main_db()

        if db not in cls.__connections:
            connection, query = cls._connect_to_db(db, cls.__config.db_root_user, cls.__config.db_root_passwd)
            cls.__connections[db] = {'connection': connection, 'query': query}

        if cls.__connections[db]['query']:
            return cls.__connections[db]['query']
        else:
            raise Exception("local db connection lost")
            
