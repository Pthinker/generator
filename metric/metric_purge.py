#-*- coding: utf-8 -*-

from file_man.jfiles import JMetricFile
from conf import ConfigReader
from db.db_conn import DBManager
import datetime
from simplejson.ordered_dict import OrderedDict
import os
from libs import is_int
import sys

class MetricPurge(object):
    """
    Internal report dashboard element
    """
    # path to directory for data and meta data
    _jfile = None
    _path = None
    _segment_values = []
    _segment = {}
    _segment_value_id = None
    _charting_intervals = []
    _logger = None

    def __init__(self, id, logger):
        self._id = id
        config = ConfigReader()
        self._db = DBManager.get_query()
        self._logger = logger
        self._data = self._get_element()
        if self._data['type'] == 'metric':
            self._path = config.metric_root
        else:
            self._path = config.multimetric_root

        self._jfile = JMetricFile(self._path, self._id)

    def _get_element(self):
        """
        fetch element info
        """

        res = self._db.Query("""
                        SELECT `element_id`, `segment_id`, `type`, `measurement_interval_id`
                        FROM dashboard_element
                        WHERE
                            dashboard_element.`element_id`=%s""",(self._id, ))
        if not res:
            raise Exception("Metric %s is not found in db" % self._id)
        data = self._db.record[0]

        if not data['segment_id']:
            data['segment_id'] = 0

        return data

    def delete_all(self):
        """
        delete all element's files
        """
        
        if self._data['segment_id']:
            segment_values = [segment_value['segment_value_id'] for segment_value in self._get_segment_values()]
        else:
            segment_values = [0]

        self._jfile.purge_files(segment_values)

    def delete_segment(self, segment_value_id):
        """
        delete all interval files for specified segment id
        """

        # get all intervals
        intervals = self._get_charting_intervals()
        for interval in intervals:
            self._jfile.purge_interval_files(interval['charting_interval_id'], segment_value_id)
        self._jfile.purge_segment_file(segment_value_id)
        self._jfile.purge_preview_files(segment_value_id)

    def delete_interval(self, charting_interval_id, segment_value_id):
        """
        delete specified interval files for all/specified segment
        """

        if segment_value_id is None:
            # fetch segments
            if self._data['segment_id']:
                segment_values = [segment_value['segment_value_id'] for segment_value in self._get_segment_values()]
            else:
                segment_values = [0]
        else:
            segment_values = [segment_value_id]
        for segment_value_id in segment_values:
            self._jfile.purge_interval_files(charting_interval_id, segment_value_id)

    def _get_charting_intervals(self):
        """
        get all charting intervals info
        """
        if self._data['measurement_interval_id']:
            self._db.Query("""SELECT charting_interval.*,
                                   measurement_interval.*,
                                   measurement_interval_charting_interval.look_ahead_percent,
                                   measurement_interval_charting_interval.measurement_interval_charting_interval_id,
                                   measurement_interval_charting_interval.index_charting_interval_ind
                                FROM measurement_interval, measurement_interval_charting_interval, charting_interval
                            WHERE
                                measurement_interval.measurement_interval_id = measurement_interval_charting_interval.measurement_interval_id
                                AND charting_interval.charting_interval_id = measurement_interval_charting_interval.charting_interval_id
                                AND measurement_interval.measurement_interval_id=%s
                            ORDER BY charting_interval.display_sequence""",(self._data['measurement_interval_id']))
            charting_intervals = [interval for interval in self._db.record]
        else:
            charting_intervals = []
        return charting_intervals

    def _get_segment_values(self):
        """
        Get segment values
        """
        segment_values = []
        if self._data['segment_id']:
            self._db.Query("""SELECT *
                            FROM segment_value
                        WHERE
                            `segment_id` = %s""",(self._data['segment_id']))
            segment_values = [segment_value for segment_value in self._db.record]
        return segment_values
