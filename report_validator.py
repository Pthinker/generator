#!/usr/bin/python2.5
#-*- coding: utf-8 -*-


import sys
import MySQLdb
from db.db_conn import DBManager
from metric.report_validator import ReportValidator
import simplejson
import traceback
from metric.logger.logger import MLogger
from metric.libs import is_int, log_traceback
import time


class Validator:
    def __init__(self):
        logger = MLogger('report validator')
        self._logger = logger.get_logger()
        try:
            self._db = DBManager().get_query()
        except MySQLdb.Error, message:
            self._logger.error("Report validator. Init mysql db manager error - %s" % message)
            log_traceback(self._logger, traceback)
            print simplejson.dumps({'status':'ERROR', 'message':'%s' % message, 'fetched_rows':''})
            sys.exit()
        except Exception, exc:
            self._logger.error("Report validator. Init mysql db manager error - %s." % exc)
            log_traceback(self._logger, traceback)
            print simplejson.dumps({'status':'ERROR', 'message':'report validation failed. %s' % exc, 'fetched_rows': ''})
            sys.exit()

    def run(self, validator_command, element_id, extra_id):
        fetched_rows = 0
        self._logger.info('Generator run: %s' % ' '.join(sys.argv))
        try:
            res = self._db.Query("""SELECT dashboard_element.element_id
                            FROM dashboard_element
                        WHERE
                            dashboard_element.`type`='internal report'
                            AND element_id = %s""", (element_id, ))
        except MySQLdb.Error, message:
            self._logger.error("Report validator. Try to get element %s. SQL error %s" % (element_id, message))
            log_traceback(self._logger, traceback)
            print simplejson.dumps({'status':'ERROR', 'message':'SQL error %s' % message, 'fetched_rows': ''})
            sys.exit()
        if not res:
            print simplejson.dumps({'status':'ERROR', 'message':'incorrect report element id', 'fetched_rows':''})
            sys.exit()
        try:
            report = ReportValidator()
            report.set_logger(self._logger)
            #report.init(element_id)
#            validator_commands = {'data_fetch': report.data_fetch,
#                                'metadata_update': report.metadata_update,
#                                'saving_chart': report.saving_chart,
#                                'data_generation': report.data_generation,
#                                'report_generation': report.report_generation,
#                                'pivot_generation': report.pivot_generation,
#                                'chart_generation': report.chart_generation,
#                                'restore_data': report.restore_data,
#                                'saving_report': report.saving_report
#                                }
#            fetched_rows = validator_commands.get(validator_command, self.unknown_command)(id1)
            command = None
            params = None
            validator_commands = {'data_fetch': (report.data_fetch, {}),
                                'metadata_update': (report.metadata_update, {}),
                                'saving_chart': (report.saving_chart, {'chart_id': extra_id}),
                                'data_generation': (report.data_generation, {}),
                                'report_generation': (report.report_generation, {}),
                                'pivot_generation': (report.pivot_generation, {'pivot_id': extra_id}),
                                'chart_generation': (report.chart_generation, {'chart_id': extra_id}),
                                'restore_validation_data': (report.restore_validation_data, {}),
                                'saving_report': (report.save_validation_data, {}),
                                'save_validation_data': (report.save_validation_data, {})
                                }
            try:
                command, params = validator_commands[validator_command]
            except ValueError:
                self.unknown_command()
            #if validator_command in ['restore_validation_data', 'saving_report', 'save_validation_data']:
            if validator_command in ['restore_validation_data']:
                report.get_data = False
            else:
                report.get_data = True

            report.init(element_id)
            if command:
                fetched_rows = command(**params)
                
        except MySQLdb.Error, message:
            self._logger.error("Report validator. Try to validate element %s. SQL error %s" % (element_id, message))
            log_traceback(self._logger, traceback)
            print simplejson.dumps({'status':'ERROR', 'message':'validation failed. SQL error %s' % message, 'fetched_rows':''})
            sys.exit()
        except Exception, exc:
            self._logger.error("Report validator. Try to validate element %s. Exception %s" % (element_id, exc))
            log_traceback(self._logger, traceback)
            print simplejson.dumps({'status':'ERROR', 'message':'validation failed. %s' % exc, 'fetched_rows': ''})
            sys.exit()

        print simplejson.dumps({'status':'OK', 'message':'report validated successfully', 'fetched_rows': fetched_rows})

    def unknown_command(self):
        print simplejson.dumps({'status': 'ERROR', 'message': 'Unknown validation command', 'fetched_rows':''})
        sys.exit()
        
if __name__ == "__main__":
    validator = Validator()
    params = sys.argv[1:]
    usage = """Usage:
report_validator.py <command> <report_id> [<chart_id> | <pivot_id>]
<command>           : available commands:
                            data_fetch: run sql/url query to get data
                            metadata_update: update report meta data (update columns, etc)
                            saving_chart: collect data for charting by selected columns/rows. <chart_id> is required.
                            data_generation: create dataset from fetched data
                            report_generation: create all enabled report's elements (pivots, charts)
                            pivot_generation: create specified pivot and its enabled charts. <pivot_id> is required.
                            chart_generation: create specified chart. <chart_id> is required.
                            restore_validation_data: restore previously saved validation files
                            saving_report: store current validation files
                            save_validation_data: store current validation files. alias for 'saving_report'
<report_id>         : process dashboard report element with specified id.
-h,--h,-help,--help : show this message.
    """

    has_error = False
    validator_command = None
    element_id = None
    extra_id = None
    if params:
        if params[0] == '-h' or params[0] == '--h' or params[0] == '-help' or params[0] == '--help':
            print usage
            sys.exit()
        validator_command = params[0]
        del(params[0])
        if params:
            if is_int(params[0]) and int(params[0]) > 0:
                element_id = int(params[0])
                del(params[0])
                if params and is_int(params[0]) and int(params[0]) > 0:
                    extra_id = int(params[0])
            else:
                has_error = True
        else:
            has_error = True
    else:
        has_error = True

    if has_error:
        print simplejson.dumps({'status':'ERROR', 'message':'incorrect arguments', 'fetched_rows':''})
        exit()

    validator.run(validator_command, element_id, extra_id)
