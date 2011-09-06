#!/usr/bin/python2.5
#-*- coding: utf-8 -*-

from db.db_conn import DBManager
from metric.logger.logger import MLogger
from metric.report_data_manager import ReportDataTableManager
import sys
from metric.libs import is_int, log_traceback
import MySQLdb
import simplejson
import traceback


class ProcessReportData(object):
    _db = None
    _logger = None
    command = ''
    table_name = ''
    element_id = 0
    report_name = None
    initial_measurement_time = None

    def __init__(self) :
        self._db = DBManager().get_query()
        logger = MLogger('report data manager')
        self._logger = logger.get_logger()

    def _check_element(self):
        try:
            res = self._db.Query("""SELECT dashboard_element.element_id
                            FROM dashboard_element
                        WHERE
                            dashboard_element.`type`='internal report'
                            AND element_id = %s
                            """, (self.element_id, ))
        except MySQLdb.Error, message:
            self._logger.error("Report generator. Try to get element %s" % self.element_id)
            log_traceback(self._logger, traceback)
            print simplejson.dumps({'status':'ERROR', 'message':'SQL error %s' % message})
            sys.exit()
        if not res:
            print simplejson.dumps({'status':'ERROR', 'message': 'Report %s not found' % self.element_id})
            sys.exit()

    def _create_table(self):
        try:
            data_manager = ReportDataTableManager(self.element_id, self._logger)
            table_name = data_manager.create_table()
        except Exception, e:
            log_traceback(self._logger, traceback)
            print simplejson.dumps({'status': 'ERROR', 'message': "%s" % e})
            sys.exit()
        print simplejson.dumps({'status': 'OK', 'message': 'Table `%s` is successfully created' % table_name})

    def _generate_data(self):
        try:
            data_manager = ReportDataTableManager(self.element_id, self._logger)
            table_name = data_manager.generate_data(self.initial_measurement_time)
        except Exception, e:
            log_traceback(self._logger, traceback)
            print simplejson.dumps({'status': 'ERROR', 'message': "%s" % e})
            sys.exit()
        print simplejson.dumps({'status': 'OK', 'message': 'Data for table `%s` is successfully generated' % table_name})

    def _test_table_name(self):
        try:
            data_manager = ReportDataTableManager(self.element_id, self._logger)
            table_name = data_manager.test_table_name(self.report_name)
        except Exception, e:
            log_traceback(self._logger, traceback)
            print simplejson.dumps({'status': 'ERROR', 'message': "%s" % e})
            sys.exit()
        print simplejson.dumps({'status': 'OK', 'message': 'Table can be successfully named as %s' % table_name})

    def _rename_table(self):
        try:
            data_manager = ReportDataTableManager(self.element_id, self._logger)
            new_table_name, old_table_name = data_manager.rename_table(self.report_name)
        except Exception, e:
            log_traceback(self._logger, traceback)
            print simplejson.dumps({'status': 'ERROR', 'message': "%s" % e})
            sys.exit()
        print simplejson.dumps({'status': 'OK', 'message': 'Table `%s` is successfully renamed to %s' % (old_table_name, new_table_name)})

    def _drop_table(self):
        try:
            data_manager = ReportDataTableManager(self.element_id, self._logger)
            table_name = data_manager.drop_table()
        except Exception, e:
            log_traceback(self._logger, traceback)
            print simplejson.dumps({'status': 'ERROR', 'message': "%s" % e})
            sys.exit()
        print simplejson.dumps({'status': 'OK', 'message': 'Table `%s` is successfully dropped' % table_name})

    def process(self):
        self._logger.info('Report Data Table Manager run: %s' % ' '.join(sys.argv))
        if self.command == 'unknown':
            print simplejson.dumps({'status':'ERROR', 'message':'Unknown command'})
        else:
            if self.command == 'create_table':
                self._check_element()
                self._create_table()
            elif self.command == 'generate_data':
                self._check_element()
                self._generate_data()
            elif self.command == 'test_table_name':
                self._test_table_name()
            elif self.command == 'rename_table':
                self._check_element()
                self._rename_table()
            elif self.command == 'drop_table':
                self._check_element()
                self._drop_table()
            else:
                print simplejson.dumps({'status':'ERROR', 'message':'Unknown command'})

            

if __name__ == "__main__":
    #print simplejson.dumps({'status': 'OK', 'message': ''})
    #sys.exit()
    data_manager = ProcessReportData()
    data_manager._logger.info('Report Data Table Manager run: %s' % ' '.join(sys.argv))
    params = sys.argv[1:]
    usage = """Usage:
report_data_manager.py <report_id> <command> <params>
<command>:
        create_table
        generate_data [<initial measurement date>]
        rename_table <old table name>
        test_table_name <table name>
        drop_table
    """
    data_manager.command = 'unknown'
    if params:
        if params[0] == '-h' or params[0] == '--h' or params[0] == '-help' or params[0] == '--help':
            print usage
            sys.exit()
        elif is_int(params[0]) and int(params[0]) > 0:
            data_manager.element_id = int(params[0])
            del(params[0])
            if params:
                if params[0] == 'create_table':
                    data_manager.command = 'create_table'
                elif params[0] == 'generate_data':
                    data_manager.command = 'generate_data'
                    del(params[0])
                    if params:
                        data_manager.initial_measurement_time = params[0]
                elif params[0] == 'rename_table' and len(params) >= 2:
                    data_manager.command = 'rename_table'
                    data_manager.report_name = params[1]
                elif params[0] == 'test_table_name' and len(params) >= 2:
                    data_manager.command = 'test_table_name'
                    data_manager.report_name = params[1]
                elif params[0] == 'drop_table':
                    data_manager.command = 'drop_table'
    data_manager.process()