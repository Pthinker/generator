#!/usr/bin/python2.5
#-*- coding: utf-8 -*-

#import os
#os.environ['PYCHECKER'] = '-q -#100'
#import pychecker.checker

from db.db_conn import DBManager
import sys
import MySQLdb
import simplejson
import traceback
from metric.logger.logger import MLogger
from metric.libs import is_int, log_traceback
from metric.report import Report
from metric.report_purge import ReportPurge

#import time

class Updater:
    element_id = None
    command = 'normal'
    segment_value_id = None
    process_dataset_ids = None
    initial_measurement_time = None
    final_measurement_time = None
    pivot_id = None
    chart_id = None
    update_commands = ['normal', 'full_gen', 'soft_gen', 'composite', 'delete_data']
    delete_commands = ['delete_all', 'delete_segment', 'delete_pivot', 'delete_chart']
    _elements = []

    def __init__(self) :
        logger = MLogger('report generator')
        self._logger = logger.get_logger()
        try:
            self._db = DBManager().get_query()
        except MySQLdb.Error, message:
            self._logger.error("Report generator. Init mysql db manager error - %s" % message)
            log_traceback(self._logger, traceback)
            print simplejson.dumps({'status': 'ERROR', 'message': '%s' % message})
            sys.exit()
        except Exception, exc:
            self._logger.error("Report generator. Init mysql db manager error - %s." % exc)
            log_traceback(self._logger, traceback)
            print simplejson.dumps({'status': 'ERROR', 'message': 'report update failed. %s' % exc})
            sys.exit()

    def run(self):
        """
        Process command
        """

        self._logger.info('Generator run: %s' % ' '.join(sys.argv))

        if self.element_id:
            # process specified report
            if self.command in self.delete_commands:
                # process delete command for specified report
                report = ReportPurge(self.element_id, self._logger)
                message = ''
                try:
                    if self.command == 'delete_all':
                        # delete whole report
                        report.delete_all()
                        message = 'Report %s deleted successfully.' % self.element_id
                    elif self.command == 'delete_segment':
                        # delete segment
                        if self.segment_value_id is None:
                            raise Exception('segment is not specified')
                        report.delete_segment(self.segment_value_id)
                        message = 'Segment %s of report %s deleted successfully.' % (self.segment_value_id, self.element_id)
                    elif self.command == 'delete_pivot':
                        # delete pivot
                        if self.pivot_id is None:
                            raise Exception('pivot is not specified')
                        report.delete_pivot(self.pivot_id)
                        message = 'Pivot %s of report %s deleted successfully.' % (self.pivot_id, self.element_id)
                    elif self.command == 'delete_chart':
                        # delete chart
                        if self.chart_id is None:
                            raise Exception('chart is not specified')
                        report.delete_chart(self.chart_id)
                        message = 'Chart %s of report %s deleted successfully.' % (self.chart_id, self.element_id)
                except MySQLdb.Error, message:
                    # process Mysql error
                    self._logger.error("Report generator. Report %s update failed. SQL error %s" % (self.element_id, message))
                    log_traceback(self._logger, traceback)
                    print simplejson.dumps({'status': 'ERROR', 'message': 'SQL error %s' % message})
                    sys.exit()
                except Exception, exc:
                    # process any other error
                    self._logger.error("Report generator. Report %s update failed. Exception %s" % (self.element_id, exc))
                    log_traceback(self._logger, traceback)
                    print simplejson.dumps({'status': 'ERROR', 'message': 'report %s update failed. %s' % (self.element_id, exc)})
                    sys.exit()
                print simplejson.dumps({'status': 'OK', 'message': message})
            else:
                # process update command for specified report
                self._run_one()
        else:
            # process update command for all reports
            self._run_all()

    def _run_one(self):
        """
        fetch specified report
        """
        try:
            res = self._db.Query("""SELECT dashboard_element.element_id
                            FROM dashboard_element
                        WHERE
                            dashboard_element.`type`='internal report'
                            AND element_id = %s
                            """, (self.element_id, )) #AND enabled_ind = 'Y'
        except MySQLdb.Error, message:
            self._logger.error("Report generator. Try to get element %s" % self.element_id)
            log_traceback(self._logger, traceback)
            print simplejson.dumps({'status': 'ERROR', 'message': 'SQL error %s' % message})
            sys.exit()
        if res:
            self._elements = [row for row in self._db.record]
            self._run()
        else:
            print simplejson.dumps({'status': 'ERROR', 'message': 'report %s updated failed. element not found' % self.element_id})

    def _run_all(self):
        """
        fetch all enabled reports
        """

        try:
            res = self._db.Query("""SELECT dashboard_element.element_id
                            FROM dashboard_element
                        WHERE
                            dashboard_element.`type`='internal report'
                            AND enabled_ind = 'Y'
                            ORDER BY element_id""")
        except MySQLdb.Error, message:
            self._logger.error("Report generator. Try to get all elements sql error - %s" % message)
            log_traceback(self._logger, traceback)
            print simplejson.dumps({'status': 'ERROR', 'message': 'SQL error %s' % message})
            sys.exit()
        if res:
            self._elements = [row for row in self._db.record]
            self._run()
        else:
            print simplejson.dumps({'status': 'ERROR', 'message': 'No reports found'})

    def _run(self):
        """
        process report(s)
        """
        for row in self._elements:
            report = None
            try:
                report = Report(self.command, self.process_dataset_ids, self.initial_measurement_time, self.final_measurement_time)
                report.set_logger(self._logger)
                #start = time.time()
                report.init(row['element_id'])
                res = report.update(self.segment_value_id)
                #print "it took", time.time() - start, "seconds."
            except MySQLdb.Error, message:
                self._logger.error("Report generator. Report %s update failed. SQL error %s" % (row['element_id'], message))
                log_traceback(self._logger, traceback)
                print simplejson.dumps({'status': 'ERROR', 'message': 'report %s update failed. SQL error %s' % (row['element_id'], message)})
                if report:
                    report.unlock()
                continue
            except Exception, exc:
                self._logger.error("Report generator. Report %s update failed. Exception %s" % (row['element_id'], exc))
                log_traceback(self._logger, traceback)
                print simplejson.dumps({'status': 'ERROR', 'message': 'report %s update failed. %s' % (row['element_id'], exc)})
                if report:
                    report.unlock()
                continue
            if res:
                print simplejson.dumps({'status': 'OK', 'message': 'report %s updated successfully. %s' % (row['element_id'], res)})
            else:
                print simplejson.dumps({'status': 'OK', 'message': 'report %s updated successfully' % row['element_id']})

if __name__ == "__main__":
    params = sys.argv[1:]
    usage = """Usage:
report_generator.py
                [<report_id>
                    [-delete_all] |
                    [-delete_pivot <pivot_id>] |
                    [-delete_chart <chart_id>] |
                    [-f [<initial_measurement_time> [-t <final_measurement_time>]]] |
                    [-d [<initial_measurement_time> [-t <final_measurement_time>]]] |
                    [-s] |
                    [-c] |
                    [<segment_value_id>
                        [-delete_segment] |
                        [-f [<initial_measurement_time> [-t <final_measurement_time>]]] |
                        [-d [<initial_measurement_time> [-t <final_measurement_time>]]] |
                        [-s [<dataset_ids>]]
                        [-c]
                    ]
                ] |
                [-h]

<report_id>         : process dashboard report element with specified id. All enabled reports are processed if <report_id> is not set.
<segment_value_id>  : process segment value if <report_id> is set. All report segments are processed if <segment_value_id> is not set.
                      Available only if <report_id> is set.
-delete_all         : delete all report files
-delete_segment     : delete all segment files. Available only if <segment_value_id> is set.
-delete_pivot       : delete all pivot files. Available only if <pivot_id> is set.
-delete_chart       : delete all chart files. Available only if <chart_id> is set.
-f                  : make "full" re-generation of report(s). Available for single report and for all reports.
-d                  : Delete instances of report. Start/end measurement date are the same as for -f.
-s                  : make "soft" (without real fetching data) re-generation of report(s). Available for single report and for all reports.
<dataset_ids>       : process "soft" re-generation of specified dataset ids. All dataset ids are processed if <dataset_ids> is not set.
                      List of ids is comma-separated, interval are available, for example: 1,2,3-5,8
                      Available only if <report_id> and <segment_value_id> are set.
-c                  : populate existing data to composite report table. Report files will not be regenerated.
-h,--h,-help,--help : show this message.
"""
    updater = Updater()

#    import hotshot
#    prof = hotshot.Profile("your_project.prof")
#    prof.start()

    if params:
        if params[0] == '-h' or params[0] == '--h' or params[0] == '-help' or params[0] == '--help':
            print usage
            sys.exit()
        if is_int(params[0]) and int(params[0]) > 0:
            # get <report_id>
            updater.element_id = int(params[0])
            del(params[0])
            if params:
                if params[0] == '-delete_all':
                    # get <report_id> -delete_all
                    updater.command = 'delete_all'
                elif len(params) >= 2 and params[0] == '-delete_pivot' and is_int(params[1]) and int(params[1]) > 0:
                    # get <report_id> -delete_pivot <pivot_id>
                    updater.command = 'delete_pivot'
                    updater.pivot_id = params[1]
                elif len(params) >= 2 and params[0] == '-delete_chart' and is_int(params[1]) and int(params[1]) > 0:
                    # get <report_id> -delete_chart <chart_id>
                    updater.command = 'delete_chart'
                    updater.chart_id = params[1]
                elif len(params) >= 4 and params[0] == '-f' and params[2] == '-t':
                    # get <report_id> -f [<from_date> [-t <to_date>]]
                    updater.command = 'full_gen'
                    updater.initial_measurement_time = params[1]
                    updater.final_measurement_time = params[3]
                elif len(params) >= 2 and params[0] == '-f':
                    # get # get <report_id> -f [<from_date>]
                    updater.command = 'full_gen'
                    updater.initial_measurement_time = params[1]
                elif params[0] == '-f':
                    # get # get <report_id> -f
                    updater.command = 'full_gen'
                elif len(params) >= 4 and params[0] == '-d' and params[2] == '-t':
                    # get <report_id> -d [<from_date> [-t <to_date>]]
                    updater.command = 'delete_data'
                    updater.initial_measurement_time = params[1]
                    updater.final_measurement_time = params[3]
                elif len(params) >= 2 and params[0] == '-d':
                    # get # get <report_id> -f [<from_date>]
                    updater.command = 'delete_data'
                    updater.initial_measurement_time = params[1]
                elif params[0] == '-d':
                    # get # get <report_id> -f
                    updater.command = 'delete_data'
                elif params[0] == '-s':
                    # get # get <report_id> -s
                    updater.command = 'soft_gen'
                    del(params[0])
                elif params[0] == '-c':
                    # get # get <report_id> -s
                    updater.command = 'composite'
                    del(params[0])
                elif is_int(params[0]):
                    # get <report_id> <segment_id>
                    updater.segment_value_id = int(params[0])
                    del(params[0])
                    if params:
                        if params[0] == '-delete_segment':
                            # get <report_id> <segment_id> -delete_segment
                            updater.command = 'delete_segment'
                        elif len(params) >= 4 and params[0] == '-f' and params[2] == '-t':
                            # get # get <report_id> <segment_id> -f [<from_date> [-t <to_date>]]
                            updater.command = 'full_gen'
                            updater.initial_measurement_time = params[1]
                            updater.final_measurement_time = params[3]
                        elif len(params) >= 2 and params[0] == '-f':
                            # get <report_id> <segment_id> -f [<from_date>]
                            updater.command = 'full_gen'
                            updater.initial_measurement_time = params[1]
                        elif params[0] == '-f':
                            # get <report_id> <segment_id> -f
                            updater.command = 'full_gen'
                        elif len(params) >= 4 and params[0] == '-d' and params[2] == '-t':
                            # get # get <report_id> <segment_id> -f [<from_date> [-t <to_date>]]
                            updater.command = 'delete_data'
                            updater.initial_measurement_time = params[1]
                            updater.final_measurement_time = params[3]
                        elif len(params) >= 2 and params[0] == '-d':
                            # get <report_id> <segment_id> -f [<from_date>]
                            updater.command = 'delete_data'
                            updater.initial_measurement_time = params[1]
                        elif params[0] == '-d':
                            # get <report_id> <segment_id> -f
                            updater.command = 'delete_data'
                        elif len(params) >= 2 and params[0] == '-s':
                            # get <report_id> <segment_id> -s <dataset_ids>
                            updater.command = 'soft_gen'
                            updater.process_dataset_ids = params[1]
                        elif params[0] == '-s':
                            # get <report_id> <segment_id> -s
                            updater.command = 'soft_gen'
                        elif params[0] == '-c':
                            # get <report_id> <segment_id> -s
                            updater.command = 'composite'

    updater.run()
#    prof.stop()
#    prof.close()
