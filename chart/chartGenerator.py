#-*- coding: utf-8 -*-

from db.db_conn import DBManager
from sys import exit
import copy
import time


class ChartGenerator:
    def __init__(self, formatter=None):
        self.formatter = formatter

    def report_charts(self, element_id, segment_value_id, meas_time, data_set_instance_id, data, jfile, chart_id):
        self._db = DBManager.get_query()
        if chart_id:
            self._db.Query("""SELECT report_data_set_chart_id FROM report_data_set_chart
                                WHERE element_id=%s AND report_data_set_chart_id = %s""", (element_id, chart_id))
        else:
            self._db.Query("""SELECT report_data_set_chart_id FROM report_data_set_chart
                                WHERE element_id=%s AND enabled_ind='Y'""", (element_id, ))
        for el in self._db.record:
            self.report_chart(el['report_data_set_chart_id'], element_id, segment_value_id, meas_time, data_set_instance_id, data, jfile, 'large')

    def _strip_total(self, data, bars_or_lines_created_for, index):
        stripped_data = data.copy()
        if bars_or_lines_created_for == 'column headers':
            rows_num = len(stripped_data[index]['rows'])
            if rows_num:
                total_cell = stripped_data[index]['rows'][rows_num - 1][0]['original_val']
                # do not remove TOTAL column if it is the only row besides label column
                if (
                    (isinstance(total_cell, str) and total_cell == 'TOTAL') or \
                    (isinstance(total_cell, unicode) and total_cell == u'TOTAL')) and \
                        rows_num > 1:
                    del(stripped_data[index]['rows'][rows_num - 1])
        else:
            col_num = len(stripped_data[index]['header'])
            total_cell = stripped_data[index]['header'][col_num - 1]['original_val']
            # do not remove TOTAL row if it is the only row besides label row
            if (
                (isinstance(total_cell, str) and total_cell == 'TOTAL') or \
                (isinstance(total_cell, unicode) and total_cell == u'TOTAL')) and \
                    col_num > 1:
                del(stripped_data[index]['header'][col_num - 1])
                for i, v in enumerate(stripped_data[index]['rows']):
                    del(stripped_data[index]['rows'][i][col_num - 1])
        return stripped_data

    def report_chart(self, chart_id, element_id, segment_value_id, meas_time, data_set_instance_id, data, jfile, type):
        from reportChart import ReportChart
        self._db = DBManager.get_query()
        self._db.Query("""SELECT report_data_set_chart_id, bars_or_lines_created_for, report_data_set_pivot_id, name
                                FROM report_data_set_chart
                                WHERE element_id=%s AND report_data_set_chart_id = %s""", (element_id, chart_id))
        chart = self._db.record[0]
        if chart['report_data_set_pivot_id']:
            index = chart['report_data_set_pivot_id']
        else:
            index = 0
        #if not data.has_key(index):
        if index not in data:
            raise Exception("There is no source data for chart %s (%s)" % (chart['name'], chart_id))

        data_chart = self._strip_total(data, chart['bars_or_lines_created_for'], index)
        _report_chart = ReportChart(chart_id, element_id, segment_value_id, meas_time, data_set_instance_id, data_chart, jfile, type)
        _report_chart.generateChart()

    def report_thumbnail(self, element_id, segment_value_id, meas_time, data_set_instance_id, data, jfile, chart_id):
        self.report_chart(chart_id, element_id, segment_value_id, meas_time, data_set_instance_id, data, jfile, 'thumbnail')

    def report_preview(self, element_id, meas_time, segment_value_id, data_set_instance_id, data, jfile, chart_id):
        self.report_chart(chart_id, element_id, segment_value_id, meas_time, data_set_instance_id, data, jfile, 'preview')

    def metric(self, metric_id, interval, data, jfile, type):
        from metricChart import MetricChart
        metric_chart = MetricChart(metric_id, interval, data, jfile, type, self.formatter)
        return metric_chart.generate_chart()

    def metric_chart(self, metric_id, interval, data, jfile):
        return self.metric(metric_id, interval, data, jfile, 'large')

    def metric_preview(self, metric_id, interval, data, jfile):
        return self.metric(metric_id, interval, data, jfile, 'preview')

    def metric_thumbnail(self, metric_id, interval, data, jfile):
        return self.metric(metric_id, interval, data, jfile, 'thumbnail')
