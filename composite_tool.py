#!/usr/bin/python2.5
#-*- coding: utf-8 -*-

from db.db_conn import DBManager
from metric.logger.logger import MLogger
from metric.report_data_manager import ReportDataTableManager
import sys
from sys import exit
import MySQLdb
import simplejson
import traceback

class CompositeTools(object):
    _db = None
    _logger = None
    def __init__(self) :
        self._db = DBManager().get_query()
        self._logger = MLogger('report data manager')

    def clear_table(self, segment_id):
        db_name = "report_data_segment_%s" % segment_id

        segment_db = DBManager().get_db_query(db_name)
        segment_db.query("""SHOW TABLES""")

        for table in segment_db.record:
            for val in table:
                if table[val] == 'segment_value':
                    continue
                segment_db.query("""DROP TABLE IF EXISTS `%s`""" % table[val])

#    def process_segments(self):
#        self._db.Query("""SELECT `segment_id`, `partition_value_type`
#                        FROM `segment`
#                        ORDER BY `segment_id`""", ())
#
#        for segment in self._db.record:
#            self._db.Query("""SELECT `value_int`, `value_varchar`, `segment_value_id`
#                        FROM `segment_value`
#                    WHERE
#                        `segment_id` = %s
#                    ORDER BY `segment_id`""", (segment['segment_id']))
#
#            segment_values = [segment_value for segment_value in self._db.record]
#            self.check_segment_table(segment, segment_values)
#        zero_segment = {
#            'segment_id': 0
#        }
#        zero_segment_values = []
#        self.check_segment_table(zero_segment, zero_segment_values)

    def clear_tables(self):
        self._db.Query("""SELECT `segment_id`, `partition_value_type`
                        FROM `segment`
                        ORDER BY `segment_id`""")

        for segment in self._db.record:
            self.clear_table(segment['segment_id'])

        self.clear_table(0)

    def process_reports(self):
        self._db.Query("""SELECT dashboard_element.element_id
                            FROM dashboard_element
                        WHERE
                            dashboard_element.`type`='internal report'
                            AND enabled_ind = 'Y'
                            ORDER BY element_id""")
        elements = [row for row in self._db.record]
        for element in elements:
            data_manager = ReportDataTableManager(element['element_id'], self._logger)
            data_manager.check_table()

if __name__ == "__main__":
    ct = CompositeTools()
    ct.clear_tables()
    ct.process_reports()
    