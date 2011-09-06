#!/usr/bin/python2.5

import sys
from db.db_conn import DBManager
import MySQLdb
import logging
import logging.handlers
import os, tempfile
from datetime import date,datetime,timedelta

class MLogger:
    def __init__(self, name):
        self._logger = logging.getLogger(name)
        self._logger.setLevel(logging.INFO)
        log_name = 'log-%s.txt' % date.today()

        full_log_dir = '/var/log/garbage_collection'
        if not os.path.isdir(full_log_dir):
            try:
                os.makedirs(full_log_dir)
            except OSError, e:
                raise Exception("cannot create dir %s. %s" % (full_log_dir, e))
            try:
                os.chmod(full_log_dir, 0777)
            except OSError, e:
                pass
        else:
            try:
                os.chmod(full_log_dir, 0777)
            except OSError, e:
                pass
        full_log_name = os.path.join(full_log_dir, log_name)

        try:
            os.chmod(full_log_name, 0777)
        except OSError:
            pass

        try:
            self._ch = logging.FileHandler(full_log_name)
        except IOError:
            tmp = tempfile.mkstemp(prefix='log_', dir = full_log_dir)
            self._ch = logging.FileHandler(tmp[1])

        self._formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s","%Y-%m-%d %H:%M:%S")
        self._ch.setFormatter(self._formatter)
        self._logger.addHandler(self._ch)

    def get_logger(self):
        return self._logger


class GarbageCollector:
    def __init__(self):
        logger = MLogger('garbage collector')
        self._logger = logger.get_logger()
        try:
            self._db = DBManager().get_query()
        except MySQLdb.Error, message:
            self._logger.error("Garbage collector. Init mysql db manager error - %s" % message)
            sys.exit()
        except Exception, exc:
            self._logger.error("Garbage collector. Init mysql db manager error - %s."% exc)
            sys.exit()

    def run(self):
        try:
            res = self._db.Query("""SELECT * FROM garbage_collection_action""")
        except MySQLdb.Error, message:
            self._logger.error("Try to get all garbage collection actions")
            self._logger.error(message)
            sys.exit()

        if res:
            actions = [row for row in self._db.record]
            for action in actions:
                self._garbage_collect(action)
        else:
            self._logger.info("No result returned from garbage_collection_action")

    def _garbage_collect(self, action):
        action_id = action["garbarge_collection_action_id"]
        name = action["name"]
        statement = action["delete_statement"]
        interval_value = action["retention_interval_value"]
        interval_unit = action["retention_interval_unit"]

        start_time = str(datetime.now())
        self._logger.info("Start garbage collecting action %s on %s..........................................." % (name, start_time))

        curtime = datetime.now()
        earliest_time = None
        
        if interval_value and interval_unit:
            if interval_unit=="hour":
                earliest_time = curtime - timedelta(hours=int(interval_value))
            elif interval_unit=="day":
                earliest_time = curtime - timedelta(days=int(interval_value))
            elif interval_unit=="week":
                earliest_time = curtime - timedelta(weeks=int(interval_value))
            elif interval_unit=="month":
                earliest_time = curtime.replace(month=curtime.month - int(interval_value))
            elif interval_unit=="year":
                earliest_time = curtime.replace(year=curtime.year - int(interval_value))
        
        isSucceed = "N"
        affectedRows = 0
        try:
            if earliest_time:
                self._db.Query(statement, (earliest_time))
            else:
                self._db.Query(statement)
            isSucceed = "Y"
            affectedRows = self._db.affectedRows
        except MySQLdb.Error, message:
            self._logger.error("Try to delete records in action %s" % (name))
            self._logger.error(message)

        end_time = str(datetime.now())
        try:
            self._db.Query( """INSERT INTO garbage_collection_action_log(garbarge_collection_action_id,delete_before_time,start_time,
                                 finish_time,rows_deleted,success_ind) VALUES (%s,%s,%s,%s,%s,%s)""", 
                                 (action_id,earliest_time,start_time,end_time,affectedRows,isSucceed) )
        except MySQLdb.Error, message:
            self._logger.error("Try to insert log into table garbage_collection_action_log on action %s" % (name))
            self._logger.error(message)

        self._logger.info("End garbage collecting action %s on %s..................................................." % (name, end_time))


if __name__ == "__main__":
    collector = GarbageCollector()
    collector.run()
