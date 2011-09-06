#!/usr/bin/python2.5
#-*- coding: utf-8 -*-

import sys
import MySQLdb
from db.db_conn import DBManager
from metric.single_metric import MetricElement
from metric.multi_metric import MultiMetricElement
from metric.metric_purge import MetricPurge
import simplejson
import traceback
from metric.logger.logger import MLogger
from metric.libs import is_int, log_traceback
#import time

class Updater:
    _elements = list()
    segment_value_id = None
    charting_interval_id = None
    index_interval_only = False
    element_id = None
    command = 'general'
    update_commands = ['general']
    delete_commands = ['delete_all', 'delete_segment', 'delete_interval']
    def __init__(self):
        logger = MLogger('metric generator')
        self._logger = logger.get_logger()
        try:
            self._db = DBManager().get_query()
        except MySQLdb.Error, message:
            self._logger.error("Metric generator. Init mysql db manager error - %s" % message)
            log_traceback(self._logger, traceback)
            print simplejson.dumps({'status':'ERROR', 'message':'%s' % message})
            sys.exit()
        except Exception, exc:
            self._logger.error("Metric generator. Init mysql db manager error - %s."% exc)
            log_traceback(self._logger, traceback)
            print simplejson.dumps({'status':'ERROR', 'message':'metric generation failed. %s' % exc})
            sys.exit()
    def run(self):
        """
        Process command
        """
        self._logger.info('Generator run: %s' % ' '.join(sys.argv))

        if self.element_id:
            # process specified metric
            if self.command in self.delete_commands:
                # process delete command
                metric = MetricPurge(self.element_id, self._logger)
                message = ''
                try:
                    if self.command == 'delete_all':
                        # delete whole metric
                        metric.delete_all()
                        message = 'Metric %s deleted successfully.' % self.element_id
                    elif self.command == 'delete_segment':
                        # delete segment
                        if self.segment_value_id is None:
                            raise Exception('segment is not specified')
                        metric.delete_segment(self.segment_value_id)
                        message = 'Segment %s of report %s deleted successfully.' % (self.segment_value_id, self.element_id)
                    elif self.command == 'delete_interval':
                        # delete interval
                        if self.charting_interval_id is None:
                            raise Exception('interval is not specified')
                        metric.delete_interval(self.charting_interval_id, self.segment_value_id)
                        message = 'Interval %s of metric %s deleted successfully.' % (self.charting_interval_id, self.element_id)
                except MySQLdb.Error, message:
                    # process Mysql error
                    self._logger.error("Metric generator. Metric %s update failed. SQL error %s" % (self.element_id, message))
                    log_traceback(self._logger, traceback)
                    print simplejson.dumps({'status':'ERROR', 'message':'SQL error %s' % message})
                    sys.exit()
                except Exception, exc:
                    # process any other error
                    self._logger.error("Metric generator. Metric %s update failed. Exception %s" % (self.element_id, exc))
                    log_traceback(self._logger, traceback)
                    print simplejson.dumps({'status':'ERROR', 'message': 'metric %s update failed. %s' % (self.element_id, exc)})
                    sys.exit()
                print simplejson.dumps({'status':'OK', 'message': message})
            else:
                # process update command for specified metric
                self._run_one()
        else:
            # process update command for all metrics
            self._run_all()
        
    def _run_one(self):
        """
        fetch specified metric
        """

        try:
            res = self._db.Query("""SELECT dashboard_element.element_id, dashboard_element.`type`
                            FROM dashboard_element
                        WHERE
                            (dashboard_element.`type`='metric' OR dashboard_element.`type`='multi-metric chart')
                            AND element_id = %s
                            """, (self.element_id, )) #AND enabled_ind = 'Y'
        except MySQLdb.Error, message:
            self._logger.error("Metric generator. Try to get element %s" % self.element_id)
            log_traceback(self._logger, traceback)
            print simplejson.dumps({'status':'ERROR', 'message':'SQL error %s' % message})
            sys.exit()

        if res:
            self._elements = [row for row in self._db.record]
            self._run()
        else:
            print simplejson.dumps({'status':'ERROR', 'message':'Metric %s updated failed. element not found' % element_id})

    def _run_all(self):
        """
        fetch all enabled metrics
        """
        try:
            res = self._db.Query("""SELECT dashboard_element.element_id, dashboard_element.`type`
                            FROM dashboard_element
                        WHERE
                            (dashboard_element.`type`='metric' OR dashboard_element.`type`='multi-metric chart')
                            AND enabled_ind = 'Y'
                            ORDER BY element_id""")
        except MySQLdb.Error, message:
            self._logger.error("Metric generator. Try to get all elements sql error - %s" % message)
            log_traceback(self._logger, traceback)
            print simplejson.dumps({'status':'ERROR', 'message':'SQL error %s' % message})
            sys.exit()
        if res:
            self._elements = [row for row in self._db.record]
            self._run()
        else:
            print simplejson.dumps({'status':'ERROR', 'message':'No metrics found'})

    def _run(self):
        """
        process metric(s)
        """
        for row in self._elements:
            metric = None
            try:
                #start = time.time()
                if row['type'] == 'metric':
                    metric = MetricElement(row['element_id'], self.index_interval_only)
                else:
                    metric = MultiMetricElement(row['element_id'], self.index_interval_only)
                metric.init()
                metric.set_logger(self._logger)
                metric.update(self.segment_value_id, self.charting_interval_id)
                #print "it took", time.time() - start, "seconds."
            except MySQLdb.Error, message:
                self._logger.error("Metric generator. Report %s update failed. SQL error %s" % (row['element_id'], message))
                log_traceback(self._logger, traceback)
                print simplejson.dumps({'status':'ERROR', 'message':'metric %s update failed. SQL error %s' % (row['element_id'], message)})
                if metric:
                    metric.unlock()
                continue
            except Exception, exc:
                self._logger.error("Metric generator. Report %s update failed. Exception %s" % (row['element_id'], exc))
                log_traceback(self._logger, traceback)
                print simplejson.dumps({'status':'ERROR', 'message':'metric %s update failed. %s' % (row['element_id'], exc)})
                if metric:
                    metric.unlock()
                continue
            
            print simplejson.dumps({'status':'OK', 'message':'metric %s updated successfully' % row['element_id']})


if __name__ == "__main__":

#    import hotshot
#    prof = hotshot.Profile("metr.prof")
#    prof.start()

    params = sys.argv[1:]
    usage = """Usage:
regenerate_element.py
                    [<metric_id>
                        [-delete_all] |
                        [-delete_interval <interval_id>] |
                        [-index] |
                        [<segment_value_id>
                            [-delete_segment] |
                            [-index] |
                            [<interval_id>]
                        ]
                    ] |
                    [-h]

<metric_id>         : process dashboard metric element with specified id. All metrics are processed if <metric_id> is not set.
<segment_value_id>  : process segment value if <metric_id> is set. All metric segments are processed if <segment_value_id> is not set.
                      Available only if <metric_id> is set.
<interval_id>       : process charting interval if <metric_id> and <segment_value_id> are set. All metric intervals are processed if <segment_value_id> is not set.
-delete_all         : delete all metric files.
-delete_segment     : delete metric segment files. Available only if <segment_id> is set.
-delete_interval    : delete metric interval files. Available only if <interval_id> is set.
-index              : process only index charting interval.
-h                  : show this message.
"""
    element_id = None
    updater = Updater()

    if params:
        if params[0] == '-h' or params[0] == '--h' or params[0] == '-help' or params[0] == '--help':
            print usage
            sys.exit()
        if is_int(params[0]) and int(params[0]) > 0:
            # get <metric_id>
            updater.element_id = int(params[0])
            del(params[0])
            if params:
                if params[0] == '-delete_all':
                    # get <metric_id> -delete_all
                    updater.command = 'delete_all'
                elif len(params) >= 2 and params[0] == '-delete_interval' and is_int(params[1]) and int(params[1]) > 0:
                    # get <metric_id> -delete_interval <interval_id>
                    updater.command = 'delete_interval'
                    updater.charting_interval_id = int(params[1])
                elif params[0] == '-index':
                    # get <metric_id> -index
                    updater.index_interval_only = True
                elif is_int(params[0]):
                    # get <metric_id> <segment_value_id>
                    updater.segment_value_id = int(params[0])
                    del(params[0])
                    if params:
                        if params[0] == '-delete_segment':
                            # get <metric_id> <segment_value_id> -delete_segment
                            updater.command = 'delete_segment'
                        elif params[0] == '-index':
                            # get <metric_id> <segment_value_id> -index
                            updater.index_interval_only = True
                        elif is_int(params[0]) and int(params[0]) > 0:
                            # get <metric_id> <segment_value_id> <interval_id>
                            updater.charting_interval_id = int(params[0])
    updater.run()
#    prof.stop()
#    prof.close()
