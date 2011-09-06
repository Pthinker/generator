#!/usr/bin/python2.5

import sys
import MySQLdb
from db.db_conn import DBManager
import logging
import logging.handlers
import os, tempfile
from datetime import date,datetime,timedelta

class MLogger:
    def __init__(self, name):
        self._logger = logging.getLogger(name)
        self._logger.setLevel(logging.INFO)
        log_name = 'log-%s.txt' % date.today()

        full_log_dir = os.path.join(os.path.split(os.path.split(os.path.split(os.path.abspath( __file__ ))[0])[0])[0], 'log/metric_archive')
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


class MetricArchiver:
    unit_value = {'minute':1, 'hour':60, 'day':1440, 'week':10080, 'month':302400, 'year':525600}

    def __init__(self):
        logger = MLogger('metric archive')
        self._logger = logger.get_logger()
        try:
            self._db = DBManager().get_query()
        except MySQLdb.Error, message:
            self._logger.error("Metric archive. Init mysql db manager error - %s" % message)
            sys.exit()
        except Exception, exc:
            self._logger.error("Metric generator. Init mysql db manager error - %s."% exc)
            sys.exit()

    def run(self):
        self._logger.info("Start archiving on %s......................................................" % (str(datetime.now())))
        try:
            res = self._db.Query("""SELECT dashboard_element.element_id
                                    FROM dashboard_element
                                    WHERE dashboard_element.`type`='metric'""")
        except MySQLdb.Error, message:
            self._logger.error("Metric archiver. Try to get metric IDs")
            self._logger.error(message)
            print(message)
            sys.exit()

        if res:
            metricIDs = [row['element_id'] for row in self._db.record]
            for metric_id in metricIDs:
                self._archive_metric(metric_id)
        else:
            self._logger.error("No result returned from dashboard_element")

        self._logger.info("Complete archiving on %s" % (str(datetime.now())))
     
    def _archive_metric(self, metric_id):
        self._logger.info("Processing metric id %s" % (metric_id))
        try:
            res = self._db.Query("""SELECT charting_interval_unit, charting_interval_value
                                      FROM measurement_interval_charting_interval mi_ci, dashboard_element metric, charting_interval ci
                                     WHERE metric.measurement_interval_id = mi_ci.measurement_interval_id
                                       AND ci.charting_interval_id = mi_ci.charting_interval_id
                                       AND metric.element_id = %s""", (metric_id))
        except MySQLdb.Error, message:
            self._logger.error("Metric archiver. Try to get charting_interval_unit, charting_interval_value for metric_id %s" % (metric_id))
            self._logger.error(message)
            print(message)
            return

        if res:
            intervals = [{'unit':row['charting_interval_unit'], 'value':row['charting_interval_value']} for row in self._db.record]
            max_interval = self._get_largest_interval(intervals)

            if max_interval is not None:
                #print(str(metric_id) + ":" + max_interval)
                max_compare_interval = self._get_max_compare_interval(metric_id)

                earliest_time = self._compute_earliest_time(max_interval, max_compare_interval)
                self._logger.info("Earliest date to keep is %s" % (earliest_time))

                #get metric_measured_value records to copy and remove
                try:
                    res = self._db.Query("""SELECT * FROM metric_measured_value 
                                            WHERE metric_id = %s 
                                            AND measurement_time<%s""",
                                            (metric_id, earliest_time) )

                except MySQLdb.Error, message:
                    self._logger.error("Metric archiver. Try to get metric_measured_value recodes to remove for metric_id %s" % (metric_id))
                    self._logger.error(message)
                    print(message)
                    return

                if res:
                    measurements = [row for row in self._db.record]
                    self._logger.info("%d records found that need to archive for metric id %d" % (len(measurements), metric_id))
                    processed_num = 0
                    for m in measurements:
                        try:
                            res = self._db.Query("""INSERT INTO metric_measured_value_archive_log(metric_instance_id, metric_id, segment_value_id,
                                              measurement_time,measurement_value_int,measurement_value_float,moving_average_value,
                                              standard_deviation_value,stoplight_compare_value,stoplight_bad_threshold_value,
                                              stoplight_good_threshold_value,measurement_stoplight_value,log_time) VALUES 
                                              (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ON DUPLICATE KEY UPDATE metric_id=%s, segment_value_id=%s,
                                              measurement_time=%s,measurement_value_int=%s,measurement_value_float=%s,moving_average_value=%s,
                                              standard_deviation_value=%s,stoplight_compare_value=%s,stoplight_bad_threshold_value=%s,
                                              stoplight_good_threshold_value=%s,measurement_stoplight_value=%s,log_time=%s
                                           """, (m['metric_measured_value_id'], m['metric_id'], m['segment_value_id'],
                                              m['measurement_time'], m['measurement_value_int'], m['measurement_value_float'], m['moving_average_value'],
                                              m['standard_deviation_value'], m['stoplight_compare_value'], m['stoplight_bad_threshold_value'],
                                              m['stoplight_good_threshold_value'], m['measurement_stoplight_value'], m['log_time'], 
                                              m['metric_id'], m['segment_value_id'],
                                              m['measurement_time'], m['measurement_value_int'], m['measurement_value_float'], m['moving_average_value'],
                                              m['standard_deviation_value'], m['stoplight_compare_value'], m['stoplight_bad_threshold_value'],
                                              m['stoplight_good_threshold_value'], m['measurement_stoplight_value'], m['log_time']) )
                        except MySQLdb.Error, message:
                            self._logger.error("Try to copy a record from metric_measured_value into metric_measured_value_archive_log")
                            self._logger.error(message)
                            #print(message)
                        
                        if res:
                            try:
                                self._logger.info("Delete sql: Delete FROM metric_measured_value WHERE metric_measured_value_id=%s" % (m['metric_measured_value_id']))
                                self._db.Query("""Delete FROM metric_measured_value WHERE metric_measured_value_id=%s""", (m['metric_measured_value_id']))
                                processed_num += 1
                            except MySQLdb.Error, message:
                                self._logger.error("Try to delete a record in table metric_measured_value")
                                self._logger.error(message)

                    self._logger.info("Processed %d records" % (processed_num))
                else:
                   self._logger.info("No record found to archive for metric_id:%s", (metric_id))
            else:
                self._logger.error("Metric archiver. max_interval is None for metric_id: %s" % (metric_id))
                return
        else:
            self._logger.info("No charting_interval_unit, charting_interval_value returned for metric_id:%d" % (metric_id))
            return

    def _compute_earliest_time(self, max_interval, max_compare_interval):
        curtime = datetime.now()
        earliest_time = None
        arr = max_interval.split(',')
        if arr[1]=="minute":
            earliest_time = curtime - timedelta(minutes=int(arr[0]))
        elif arr[1]=="hour":
            earliest_time = curtime - timedelta(hours=int(arr[0]))
        elif arr[1]=="day":
            earliest_time = curtime - timedelta(days=int(arr[0]))
        elif arr[1]=="week":
            earliest_time = curtime - timedelta(weeks=int(arr[0]))
        elif arr[1]=="month":
            #earliest_time = curtime - timedelta(months=arr[0])
            earliest_time = curtime.replace(month=curtime.month - int(arr[0]))
        elif arr[1]=="year":
            #earliest_time = curtime - timedelta(years=arr[0])
            earliest_time = curtime.replace(year=curtime.year - int(arr[0]))

        if max_compare_interval is not None:
            arr = max_compare_interval.split(',')
            if arr[1]=="minute":
                earliest_time = earliest_time + timedelta(minutes=int(arr[0]))
            elif arr[1]=="hour":
                earliest_time = earliest_time + timedelta(hours=int(arr[0]))
            elif arr[1]=="day":
                earliest_time = earliest_time + timedelta(days=int(arr[0]))
            elif arr[1]=="week":
                earliest_time = earliest_time + timedelta(weeks=int(arr[0]))
            elif arr[1]=="month":
                #earliest_time = earliest_time + timedelta(months=arr[0])
                earliest_time = earliest_time.replace(month=earliest_time.month + int(arr[0]))
            elif arr[1]=="year":
                #earliest_time = earliest_time + timedelta(years=arr[0])
                earliest_time = earliest_time.replace(year=earliest_time.year + int(arr[0]))
        return earliest_time

    def _get_max_compare_interval(self, metric_id):
        max_compare_interval = None
        try:
            res = self._db.Query("""SELECT compare_interval_value, compare_interval_unit
                                      FROM metric_chart_compare_line mcl, measurement_interval_charting_interval mi_ci,
                                           compare_line cl,dashboard_element metric
                                     WHERE metric.measurement_interval_id = mi_ci.measurement_interval_id
                                       AND mi_ci.measurement_interval_charting_interval_id = mcl.measurement_interval_charting_interval_id
                                       AND cl.compare_line_id = mcl.compare_line_id
                                       AND metric.element_id = %s
                                       AND compare_interval_unit IS NOT NULL""", (metric_id))
        except MySQLdb.Error, message:
            self._logger.error("Metric archiver. Try to get compare_interval_value, compare_interval_unit for metric_id %s" % (metric_id))
            self._logger.error(message)

        if res:
            compare_intervals = [{'unit':row['compare_interval_unit'].lower(), 'value':row['compare_interval_value']} for row in self._db.record]
            max_compare_interval = self._get_largest_interval(compare_intervals)
        else:
            self._logger.info("Metric archiver. No compare_interval_value, compare_interval_unit found for metric_id: %s" % (metric_id))
 
        return max_compare_interval


    def _get_largest_interval(self, intervals):
        max_value = 0
        max_interval = None
        for interval in intervals:
            value = self.unit_value[interval['unit']] * interval['value']
            if value>max_value:
                max_value = value
                max_interval = str(interval['value']) + "," + interval['unit']
        return max_interval


if __name__ == "__main__":
    archiver = MetricArchiver()
    archiver.run()
    
