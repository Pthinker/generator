#-*- coding: utf-8 -*-

from file_man.jfiles import JFile
from conf import ConfigReader
from db.db_conn import DBManager
import datetime
from simplejson.ordered_dict import OrderedDict
import os
from libs import is_int
import sys

class ReportPurge(object):
    """
    Internal report dashboard element
    """
    # path to directory for data and meta data
    _jfile = None
    _path = None
    _segment_values = []
    _segment = {}
    _segment_value_id = None
    _charts = []
    _pivots = []
    _logger = None
    _db = None

    def __init__(self, id, logger):

        self._id = id
        self._db = DBManager.get_query()
        self._data = self._get_element()
        config = ConfigReader()
        self._path = config.report_root
        self._logger = logger
        self._jfile = JFile(self._path, self._id, None)

    def delete_all(self):
        """
        Removes all element's files
        """

        if self._data['segment_id']:
            segment_values = [segment_value['segment_value_id'] for segment_value in self._get_segment_values()]
        else:
            segment_values = [0]

        self._jfile.purge_files(segment_values)

    def delete_segment(self, segment_value_id):
        """
        Removes all existing instances of charts, pivots, data sets from db for selected segment
        """

        # get all pivots
        pivots = self._get_pivots()

        # get all charts
        charts = self._get_charts()

        # get dataset instances ids
        report_data_set_instance_ids = self._get_data_set_instance_ids(segment_value_id)

        self._delete_charts(charts, segment_value_id, report_data_set_instance_ids)

        self._delete_pivots(pivots, segment_value_id, report_data_set_instance_ids)

#        # remove dataset instances from DB
#        self._db.Query("""DELETE FROM report_data_set_instance
#                                WHERE element_id = %s
#                                    AND segment_value_id = %s""", (self._id, segment_value_id))
        # remove dataset instances files
        self._jfile.purge_dataset_files(report_data_set_instance_ids, segment_value_id)
        self._jfile.purge_preview_files(segment_value_id)

    def delete_pivot(self, pivot_id):
        """
        Removes all existing instances of pivot and pivot charts
        """
        
        # get all pivots
        pivots = self._get_pivots(pivot_id)
        if pivots:
            pivot = pivots[0]
        else:
            raise Exception("Pivot %s is not found in db" % pivot_id)

        # get all charts
        charts = self._get_charts(pivot_id=pivot_id)

        # fetch segments
        if self._data['segment_id']:
            segment_values = self._get_segment_values()
        else:
            segment_values = [0]

        for segment_value in segment_values:
            if segment_value:
                segment_value_id = segment_value['segment_value_id']
            else:
                segment_value_id = 0
            report_data_set_instance_ids = self._get_data_set_instance_ids(segment_value_id)
            self._delete_charts(charts, segment_value_id, report_data_set_instance_ids)
            self._delete_pivots([pivot], segment_value_id, report_data_set_instance_ids)

    def delete_chart(self, chart_id):
        """
        Removes chart
        """
        # get all pivots
        charts = self._get_charts(chart_id=chart_id)
        if charts:
            chart = charts[0]
        else:
            raise Exception("Chart %s is not found in db" % chart_id)

        # fetch segments
        if self._data['segment_id']:
            segment_values = self._get_segment_values()
        else:
            segment_values = [0]

        for segment_value in segment_values:
            if segment_value:
                segment_value_id = segment_value['segment_value_id']
            else:
                segment_value_id = 0
            report_data_set_instance_ids = self._get_data_set_instance_ids(segment_value_id)
            self._delete_charts([chart], segment_value_id, report_data_set_instance_ids)

    def _get_charts(self, pivot_id=None, chart_id=None):
        """
        Returns charts with info.
        If pivot_id is specified returns only charts for this pivot
        If chart_id is specified returns only this chart
        """
        charts = []
        params = [self._id]
        sql = """SELECT report_data_set_chart.*,
                                    IFNULL(report_data_set_chart.report_data_set_pivot_id, 0),
                                    report_data_set_pivot.pivot_column_value_column_id,
                                    report_data_set_pivot.pivot_row_value_column_id,
                                    report_data_set_column.column_name
                                FROM report_data_set_chart
                                LEFT JOIN report_data_set_column
                                    ON report_data_set_chart.chart_by_report_data_set_column_id = report_data_set_column.report_data_set_column_id
                                LEFT JOIN report_data_set_pivot ON report_data_set_pivot.report_data_set_pivot_id = report_data_set_chart.report_data_set_pivot_id
                            WHERE
                                report_data_set_chart.`element_id`=%s"""
        if pivot_id is not None:
            sql +=  """ AND report_data_set_chart.report_data_set_pivot_id = %s"""
            params.append(pivot_id)

        if chart_id is not None:
            sql +=  """ AND report_data_set_chart.report_data_set_chart_id = %s"""
            params.append(chart_id)

        self._db.Query(sql, tuple(params))

        for chart in self._db.record:
            if not chart['report_data_set_pivot_id']:
                chart['report_data_set_pivot_id'] = 0
            charts.append(chart)
        return charts

    def _get_element(self):
        """
        Returns element info
        """
        res = self._db.Query("""
                        SELECT `element_id`, `segment_id` 
                            FROM dashboard_element
                        WHERE
                            dashboard_element.`element_id`=%s""",(self._id, ))
        if not res:
            raise Exception("Metric %s is not found in db" % self._id)
        data = self._db.record[0]

        return data

    def _get_pivots(self, pivot_id=None):
        """
        Returns pivots info
        """
        if pivot_id is not None:
            self._db.Query("""SELECT *
                                FROM report_data_set_pivot
                            WHERE
                                element_id = %s AND report_data_set_pivot_id = %s""", (self._id, pivot_id))
        else:
            self._db.Query("""SELECT *
                                FROM report_data_set_pivot
                            WHERE
                                `element_id` = %s""", (self._id, ))
        pivots = [pivot for pivot in self._db.record]
        return pivots

    def _get_segment_values(self):
        """
        Returns segment values
        """
        segment_values = []
        if self._data['segment_id']:
            self._db.Query("""SELECT *
                            FROM segment_value
                        WHERE
                            `segment_id` = %s""",(self._data['segment_id']))

            segment_values = [segment_value for segment_value in self._db.record]
        return segment_values


    def _get_data_set_instance_ids(self, segment_value_id):
        """
        Returns dataset instances ids
        """
        self._db.Query("""SELECT report_data_set_instance_id
                                FROM report_data_set_instance
                                    WHERE element_id = %s
                                        AND segment_value_id = %s""", (self._id, segment_value_id))

        return [row['report_data_set_instance_id'] for row in self._db.record]

    def _delete_charts(self, charts, segment_value_id, report_data_set_instance_ids):
        """
        Removes charts
        """
        for chart in charts:
#            # remove annotation charts instance
#            self._db.Query("""DELETE FROM report_data_set_chart_annotation_instance
#                                WHERE report_data_set_chart_instance_id IN
#                                    (SELECT report_data_set_chart_instance_id
#                                        FROM report_data_set_chart_instance
#                                            WHERE report_data_set_chart_id = %s AND
#                                                report_data_set_instance_id IN (
#                                                    SELECT report_data_set_instance_id
#                                                        FROM report_data_set_instance
#                                                            WHERE element_id = %s
#                                                                AND segment_value_id = %s
#                                                ))""", (chart['report_data_set_chart_id'], self._id, segment_value_id))
#
            # remove chart instances files
            self._jfile.purge_chart_files(chart['report_data_set_chart_id'], report_data_set_instance_ids, segment_value_id)
            # remove charts instances from DB
#            self._db.Query("""DELETE FROM report_data_set_chart_instance
#                                WHERE report_data_set_chart_id = %s AND
#                                    report_data_set_instance_id IN (
#                                        SELECT report_data_set_instance_id
#                                            FROM report_data_set_instance
#                                                WHERE element_id = %s
#                                                    AND segment_value_id = %s
#                                    )""", (chart['report_data_set_chart_id'], self._id, segment_value_id))

    def _delete_pivots(self, pivots, segment_value_id, report_data_set_instance_ids):
        """
        Removes pivots
        """
        for pivot in pivots:
            # remove chart instances files
            self._jfile.purge_pivot_files(pivot['report_data_set_pivot_id'], report_data_set_instance_ids, segment_value_id)
#            # remove pivots instances from DB
#            self._db.Query("""DELETE FROM report_data_set_pivot_instance
#                                WHERE report_data_set_pivot_id = %s AND
#                                    report_data_set_instance_id IN (
#                                        SELECT report_data_set_instance_id
#                                            FROM report_data_set_instance
#                                                WHERE element_id = %s
#                                                    AND segment_value_id = %s
#                                    )""", (pivot['report_data_set_pivot_id'], self._id, segment_value_id))
