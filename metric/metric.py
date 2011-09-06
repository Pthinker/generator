# -*- coding: utf-8 -*-

from file_man.jfiles import JMetricFile
from conf import ConfigReader
from db.db_conn import DBManager
import datetime
from chart.chart_generator import ChartGenerator
from simplejson.ordered_dict import OrderedDict
from formatter import FieldFormatter
import copy
from time import mktime
from chart.font_manager import FontManager
from operator import itemgetter
from file_man.flocker import FileLock

import time
import pprint
from sys import exit

class AbstractMetricElement:
    """
    Abstract metric dashboard element
    """
    _path = ''
    _type = ''
    _charting_intervals = list()
    _id = 0
    _jfile = None
    _segment_value_id = 0
    _segment_value = 0
    formatted_headers = list()
    orig_headers = list()
    _data = None
    _expired_date = None
    
    index_interval_only = False
    index_interval_id = 0
    _show_stop_light = False
    _show_std_deviation = False
    _show_moving_average = False
    
    _filelocker = None
    _logger = None
    formatter = None
        
    def __init__(self, id, index_interval_only):
        self.config = ConfigReader()
        self._db = DBManager.get_query()
        
        self._id = id
        self.index_interval_only = index_interval_only
        # fetch all element data
        self._data = self._get_element()

    def init(self):
        self.formatter = FieldFormatter(self._data['def_date_mask_id'])

        # fetch segments
        self._segment = self._get_segment()
        self._segment_values = self._get_segment_values()

        # get all charting intervals
        self._charting_intervals = self._get_charting_intervals()
        self._date_format_rule = self._get_date_format_rule()
        
        self.formatter.set_custom_date_format_rule(self._date_format_rule)

        if self._data['metric_stoplight_calc_method'] and self._data['metric_stoplight_calc_method'] != 'None':
            self._show_stop_light = True

        self._jfile = JMetricFile(self._path, self._id)

        self._jfile.set_segment(self._segment)

    def _get_segment(self):
        """
        Get segment
        """
        segment = list()
        if self._data['segment_id']:
            res = self._db.Query("""SELECT *
                            FROM segment
                        WHERE
                            `segment_id`=%s""",(self._data['segment_id']))
            if res:
                segment = self._db.record[0]
                if not segment['segment_value_prefix']:
                    segment['segment_value_prefix'] = ''
                if not segment['segment_value_suffix']:
                    segment['segment_value_suffix'] = ''
        return segment

    def _get_segment_values(self):
        """
        Get segment values
        """
        segment_values = list()
        if self._segment:
            res = self._db.Query("""SELECT *
                            FROM segment_value
                        WHERE
                            `segment_id`=%s""", (self._segment['segment_id']))
            if res:
                segment_values = [segment for segment in self._db.record]
        return segment_values

    def _update_run_time(self):
        """
        Update last run time, last measurement time, last_display_generation_time
        """
        self._db.Query("""INSERT INTO last_dashboard_element_segment_value
                        SET last_display_generation_time = NOW(),
                            element_id = %s,
                            segment_value_id = %s
                        ON DUPLICATE KEY UPDATE 
                            last_display_generation_time = NOW()
                            """,(self._id, self._segment_value_id))
        self._db.Query("""UPDATE dashboard_element
                        SET last_run_time = NOW()
                        WHERE
                            `element_id` = %s""", (self._id, ))

    def _get_date_format_rule(self):
        """
        Get auto format date rule
        """
        date_format_rule = ''
        if self._data['measurement_interval_id']:
            res = self._db.Query("""SELECT measurement_interval.*
                    FROM measurement_interval
                WHERE
                    `measurement_interval_id`=%s""",(self._data['measurement_interval_id']))
            if res:
                data = self._db.record[0]
                date_format_rule = data['date_format_string']
        if not date_format_rule:
            date_format_rule = '%Y-%m-%d %T'
        return date_format_rule



    def _get_element(self):
        """
        fetch element info
        """
        self._db.Query("""
                        SELECT dashboard_element.*,
                               topic.name AS topic_name,
                               dashboard_category.category,
                               measurement_interval.measurement_interval_button_name,
                               measurement_interval.chart_x_axis_label,
                               measurement_interval.alert_prior_measurement_value_count,
                               measurement_interval.interval_unit,
                               chart_layout.week_display_prefix,
                               chart_layout.show_expired_zone_ind,
                               measurement_interval.display_mask_id AS def_date_mask_id
                        FROM dashboard_element
                            LEFT JOIN topic
                                ON topic.topic_id=dashboard_element.primary_topic_id
                            LEFT JOIN chart_layout
                                ON chart_layout.layout_id=dashboard_element.chart_layout_id
                            LEFT JOIN measurement_interval
                                ON measurement_interval.measurement_interval_id=dashboard_element.measurement_interval_id
                            LEFT JOIN dashboard_category
                                ON dashboard_category.category_id=dashboard_element.category_id
                        WHERE
                            dashboard_element.`element_id`=%s""",(self._id, ))

        data = self._db.record[0]
        
        if not data['segment_id']:
            data['segment_id'] = 0
          
        return data


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

            charting_intervals = list(interval for interval in self._db.record)
            if not charting_intervals:
                raise Exception("No charting intervals for %s (%s) available" % (self._data['name'], self._id))
            if not any(row['index_charting_interval_ind'] == 'Y' for row in charting_intervals):
                charting_intervals[0]['index_charting_interval_ind'] = 'Y'
            
            # get index charting interval id
            index_interval = filter(lambda charting_int: charting_int['index_charting_interval_ind'] == 'Y', charting_intervals)
            self.index_interval_id = index_interval[0]['charting_interval_id']
        else:
            raise Exception("measurement_interval_id for %s (%s) is missing" % (self._data['name'], self._id))
        return charting_intervals

    def get_interval_xtd_start_date(self, charting_interval, end_date):
        """
        get start date
        """
        start_date = end_date
        
        if not isinstance(start_date, datetime.datetime):
            start_date = datetime.datetime.combine(start_date, datetime.time.min)
        else:
            start_date = start_date.replace(hour = 0, minute = 0, second = 0, microsecond = 0)
        
        if charting_interval['interval_unit'] == 'year':
            start_date = start_date.replace(month = 1, day = 1)
        elif charting_interval['interval_unit'] == 'month':
            res = self._db.Query("""SELECT first_day_of_month
                                        FROM calendar_month
                                    WHERE
                                        `first_day_of_month` <= %s
                                    ORDER BY `month_id` DESC
                                    LIMIT 0, 1""", (end_date,))
            if res:
                date = self._db.record[0]
                start_date = date['first_day_of_month']
            else:
                start_date.replace(day = 1)
        elif charting_interval['interval_unit'] == 'quarter':
            res = self._db.Query("""SELECT first_day_of_quarter
                                        FROM calendar_quarter
                                    WHERE
                                        `first_day_of_quarter` <= %s
                                    ORDER BY `quarter_id` DESC
                                    LIMIT 0, 1""", (end_date, ))
            if res:
                date = self._db.record[0]
                start_date = date['first_day_of_quarter']
            else:
                # get current quarter 0-3
                current_quarter = (start_date.month-1) // 3
                current_first_month_of_quarter = current_quarter * 3 + 1
                start_date = start_date.replace(day=1, month=current_first_month_of_quarter)
        elif charting_interval['interval_unit'] == 'week':
            if self._data['week_display_prefix'] == 'week starting':
                compare_week_day = 'first_day_of_week'
            else:
                compare_week_day = 'last_day_of_week'
            res = self._db.Query("""SELECT `first_day_of_week`,
                                           `last_day_of_week`
                                        FROM calendar_week
                                    WHERE
                                        `%s` <= %%s
                                    ORDER BY `week_id` DESC
                                    LIMIT 0, 1""" % compare_week_day, (end_date, ))

            if res:
                date = self._db.record[0]
                if self._data['week_display_prefix'] == 'week starting':
                    start_date = date['first_day_of_week']
                else:
                    start_date = date['last_day_of_week']
            else:
                prev_week = end_date - datetime.timedelta(days = 7)
                prev_week_day_of_week = prev_week.weekday()
                if self._data['week_display_prefix'] == 'week starting':
                    start_date = prev_week - datetime.timedelta(days=prev_week_day_of_week)
                else:
                    start_date = prev_week + datetime.timedelta(days=(6-prev_week_day_of_week))
        
        if isinstance(start_date, datetime.datetime):
            start_date = start_date.replace(hour = 0, minute = 0, second = 0, microsecond = 0)
        else:
            start_date = datetime.datetime.combine(start_date, datetime.time.min)
        
        return start_date   
    
    def get_interval_start_date(self, charting_interval, end_date):
        """
        get start date for selected charting interval
        """
        charting_interval_unit = charting_interval['charting_interval_unit'] 
        charting_interval_value = charting_interval['charting_interval_value']
        
        if charting_interval_unit == 'week':
            charting_interval_unit = 'day'
            charting_interval_value = charting_interval['charting_interval_value']*7

        self._db.Query("""SELECT DATE_SUB(%s, INTERVAL %s """ + charting_interval_unit + """) AS start_date""",
                            (end_date, charting_interval_value))
        res = self._db.record[0]

        start_date = res['start_date']
        # convert to datetime type
        if isinstance(start_date, str):
            try:
                start_date_val = datetime.datetime.strptime(start_date, '%Y-%m-%d %H:%M:%S')
            except ValueError:
                try:
                    start_date_val = datetime.datetime.strptime(start_date, '%Y-%m-%d')
                except ValueError:
                    raise Exception("Cannot format end of calculated charting interval '%s' to datetime for metric %s (%s)" % (start_date, self._data['name'], self._id))
        elif isinstance(start_date, datetime.date):
            start_date_val = datetime.datetime.combine(start_date, datetime.time.min)
        elif isinstance(start_date, datetime.datetime):
            start_date_val = start_date
        else:
            # it shouldn't be so. raise exception
            start_date_val = datetime.datetime(1900, 1, 1, 0, 0, 0) #datetime.datetime.min
        
        return start_date_val         
        
    def get_interval_end_date(self, charting_interval, last_date):
        """
        get end date for selected charting interval
        """
        end_date = datetime.datetime.today()
        # minute
        if charting_interval['interval_unit'] == 'minute':
            end_date = last_date - datetime.timedelta(minutes = 1)
            if isinstance(end_date, datetime.datetime):
                end_date = end_date.replace(second = 0, microsecond = 0)

        # hour
        elif charting_interval['interval_unit'] == 'hour':
            end_date = last_date - datetime.timedelta(hours=1)
            if isinstance(end_date, datetime.datetime):
                end_date = end_date.replace(minute = 0, second = 0, microsecond = 0)

        # day
        elif charting_interval['interval_unit'] == 'day':
            end_date = last_date - datetime.timedelta(days=1)
            if isinstance(end_date, datetime.datetime):
                end_date = end_date.replace(hour = 0, minute = 0, second = 0, microsecond = 0)

        # month
        elif charting_interval['interval_unit'] == 'month':
            res = self._db.Query("""SELECT first_day_of_month
                                        FROM calendar_month
                                    WHERE
                                        `last_day_of_month` < %s
                                    ORDER BY `month_id` DESC
                                    LIMIT 0, 1""", (last_date,))
            if res:
                date = self._db.record[0]
                end_date = date['first_day_of_month']
            else:
                end_date = datetime.datetime.today() - datetime.timedelta(months=1)
                end_date = end_date.replace(day=1)
            if isinstance(end_date, datetime.datetime):
                end_date = end_date.replace(hour = 0, minute = 0, second = 0, microsecond = 0)

        # week
        elif charting_interval['interval_unit'] == 'week':
            if self._data['week_display_prefix'] == 'week starting':
                compare_week_day = 'first_day_of_week'
            else:
                compare_week_day = 'last_day_of_week'
            res = self._db.Query("""SELECT `first_day_of_week`,
                                           `last_day_of_week`
                                        FROM calendar_week
                                    WHERE
                                        `%s` < %%s
                                    ORDER BY `week_id` DESC
                                    LIMIT 0, 1""" % compare_week_day, (last_date, ))

            if res:
                date = self._db.record[0]
                if self._data['week_display_prefix'] == 'week starting':
                    end_date = date['first_day_of_week']
                else:
                    end_date = date['last_day_of_week']
            else:
                prev_week = last_date - datetime.timedelta(days = 7)
                prev_week_day_of_week = prev_week.weekday()
                if self._data['week_display_prefix'] == 'week starting':
                    end_date = prev_week - datetime.timedelta(days = prev_week_day_of_week)
                else:
                    end_date = prev_week + datetime.timedelta(days = (6 - prev_week_day_of_week))
            
            if isinstance(end_date, datetime.datetime):
                end_date = end_date.replace(hour = 0, minute = 0, second = 0, microsecond = 0)

        # quarter
        elif charting_interval['interval_unit'] == 'quarter':
            res = self._db.Query("""SELECT first_day_of_quarter
                                        FROM calendar_quarter
                                    WHERE
                                        `last_day_of_quarter` < %s
                                    ORDER BY `quarter_id` DESC
                                    LIMIT 0, 1""", (last_date, ))
            if res:
                date = self._db.record[0]
                end_date = date['first_day_of_quarter']
            else:
                today = last_date
                current_month = today.month
                current_quarter = current_month//4

                current_first_month_of_quarter = current_quarter*3+1
                current_quarter_first_day = today.replace(day=1, month=current_first_month_of_quarter)

                year, month, day = current_quarter_first_day.timetuple()[:3]
                new_month = month - 6

                end_date = datetime.datetime(year + (new_month / 12), new_month % 12, day)
            if isinstance(end_date, datetime.datetime):
                end_date = end_date.replace(hour = 0, minute = 0, second = 0, microsecond = 0)
        # quarter
        elif charting_interval['interval_unit'] == 'year':
            today = last_date
            year_beginning = today.replace(day = 1, month = 1, hour = 0, minute = 0, second = 0)
            if last_date > year_beginning:
                end_date = year_beginning
            else:
                end_date = year_beginning.replace(year = year_beginning.year - 1)

            if isinstance(end_date, datetime.datetime):
                end_date = end_date.replace(microsecond = 0)
            
        end_date_val = None
        
        # convert to datetime type
        if isinstance(end_date, str):
            try:
                end_date_val = datetime.datetime.strptime(end_date, '%Y-%m-%d %H:%M:%S')
            except ValueError:
                try:
                    end_date_val = datetime.datetime.strptime(end_date, '%Y-%m-%d')
                except ValueError:
                    raise Exception("Cannot format beginning of calculated charting interval '%s' to datetime for metric %s (%s)" % (end_date, self._data['name'], self._id))
        elif isinstance(end_date, datetime.datetime):
            end_date_val = end_date  
        elif isinstance(end_date, datetime.date):
            end_date_val = datetime.datetime.combine(end_date, datetime.time.min)
        else:
            # it shouldn't be so
            end_date_val = datetime.datetime.today()

        
        return end_date_val         
    
    
    def get_interval(self, charting_interval):
        """
        get time interval (start and end date) for selected charting interval
        """
        # default from date
        default_end_date = datetime.datetime.today()
        if charting_interval['xtd_interval_ind'] == 'Y':
            charting_interval['interval_unit'] = 'day'
        
        # last date of charting interval period
        end_date_val = self.get_interval_end_date(charting_interval, default_end_date)
        
        if charting_interval['xtd_interval_ind'] == 'Y':
            start_date_val = self.get_interval_xtd_start_date(charting_interval, end_date_val)
        else:
            # first date of charting interval period
            start_date_val = self.get_interval_start_date(charting_interval, end_date_val)
        
        # fetch all dates where measured values supposed to be computed  
        dates = list()
        dates.append(end_date_val)
        next_end_date_val = end_date_val
        while next_end_date_val > start_date_val:
            next_end_date_val = self.get_interval_end_date(charting_interval, next_end_date_val)
            if next_end_date_val < start_date_val:
                break
            dates.append(next_end_date_val)
        dates.reverse()
                
        self._expired_date = None

        if self._data['show_expired_zone_ind'] and self._data['max_time_before_expired_sec']:
            today = datetime.datetime.today()
            
            expired_date = today - datetime.timedelta(seconds = self._data['max_time_before_expired_sec'])
            
            # if expired date is in interval. if not then do not draw expired zone
            if expired_date < end_date_val:
                self._expired_date = expired_date
            
        return end_date_val, start_date_val, dates

    def _get_y_axis_format(self, mask_id):
        """
        get y axis format
        """
        format = dict()
        if mask_id:
            self._db.Query("""SELECT *
                                FROM display_mask
                            WHERE
                                display_mask_id=%s""", (mask_id, ))
            format = self._db.record[0]
            if not format['display_precision_digits']:
                format['display_precision_digits'] = 0
            if not format['thousands_delimiter']:
                format['thousands_delimiter'] = ''
            if not format['decimal_point_delimiter']:
                format['decimal_point_delimiter'] = ''
            if not format['prefix']:
                format['prefix'] = ''
            if not format['suffix']:
                format['suffix'] = ''
        return format

    def prepare_metric(self):
        """
        to be implemented in metric/multimetric class
        """
        pass

    def get_min_max_ever_from_db(self):
        """
        to be implemented in metric/multimetric class
        """
        pass

    def create_meta(self):
        """
        data for creating main meta file
        """
        self._update_run_time()
        available_intervals = list()
        drill_to = list()
        related = list()

        metrics = self.prepare_metric()

        # see related
        res = self._db.Query("""SELECT e.*
                                    FROM dashboard_element_topic det, dashboard_element e
                                WHERE e.element_id = det.dashboard_element_id
                                    AND dashboard_element_id <> %s
                                    AND e.enabled_ind = 'Y'
                                    AND topic_id IN (select topic_id from dashboard_element_topic where dashboard_element_id = %s)
                                    AND IFNULL(e.segment_id,0) = %s
                            """,
                        (self._id, self._id, self._data['segment_id']))
        if res:
            related = list(related_element for related_element in self._db.record)

        # available measurement intervals
        res = self._db.Query("""
                        SELECT measurement_interval.*,
                                 dashboard_element.element_id
                            FROM dashboard_element
                            LEFT JOIN measurement_interval
                                ON measurement_interval.measurement_interval_id = dashboard_element.measurement_interval_id
                        WHERE
                            1
                            AND dashboard_element.shared_measure_id = %s
                            AND dashboard_element.`type` = 'metric'
                            AND IFNULL(dashboard_element.segment_id,0) = %s
                        GROUP BY measurement_interval.measurement_interval_id
                        ORDER BY
                                measurement_interval.display_sequence,
                                dashboard_element.name ASC
                        """, (self._data['shared_measure_id'], self._data['segment_id']))
        if res:
            for interval in self._db.record:
                interval['report_data_set_instance_id'] = 0
                available_intervals.append(interval)

        # drill to
        res = self._db.Query("""
                        SELECT dashboard_element.*,
                                 metric_drill_to_report.*
                            FROM metric_drill_to_report
                            LEFT JOIN dashboard_element
                                ON dashboard_element.element_id = metric_drill_to_report.report_element_id
                        WHERE
                            metric_drill_to_report.metric_element_id = %s
                            AND IFNULL(dashboard_element.segment_id,0) = %s
                        ORDER BY
                                metric_drill_to_report.display_sequence
                        """, (self._id, self._data['segment_id']))
        if res:
            for drill_report in self._db.record:
                drill_to.append(drill_report)

        available_views = list()
        available_views.append('standard')
        if self._show_stop_light:
            available_views.append('stoplight') 

        if self._show_std_deviation:
            available_views.append('std_deviation')

        # last_updated_time
        res = self._db.Query("""SELECT * FROM last_dashboard_element_segment_value WHERE element_id = %s AND segment_value_id = %s
                            """,
                        (self._id, self._data['segment_id']))
        if res:
            self._data['last_updated_time'] = self._db.record[0]['last_updated_time']
        else:
            self._data['last_updated_time'] = ''
        self._jfile.make_meta(self._data, self._charting_intervals, available_intervals, drill_to, related, metrics, self._segment_values, available_views)
    
    def fetch_compare_lines(self, charting_interval):
        """
        get all compare lines info
        """
        compare_lines = list()
        if charting_interval:
            res = self._db.Query("""SELECT
                                compare_line.*
                            FROM metric_chart_compare_line, compare_line
                            WHERE
                                metric_chart_compare_line.compare_line_id=compare_line.compare_line_id
                            AND
                                metric_chart_compare_line.measurement_interval_charting_interval_id = %s
                            ORDER BY display_sequence
                                """,(charting_interval['measurement_interval_charting_interval_id']))
            if res:
                for compare_line in self._db.record:
                    compare_lines.append(compare_line)
        return compare_lines
    
    def init_charting_data(self, compare_lines):
        """
        Create empty dictionary with metric info for charting 
        """
        data = dict()

        # formatted header
        data['header'] = list()

        # original unformatted header
        data['orig_header'] = list()

        # proportional header. needed for correct x-axis charting thinned down values
        data['even_header'] = list()

        # lists of delta between first date and other dates of headers. needed for calculation simplificated line for thinning down
        data['x_axis'] = list()

        # rows
        data['rows'] = dict()

        # list of annotations markers(True/False) - if any annotation exists for value
        data['annotations'] = dict()
        data['range_annotations'] = dict()
        
        data['thin_by_metric_id'] = 0
        
        # set compare lines
        data['compare_lines'] = list()        
        for compare_line in compare_lines:
            data['compare_lines'].append(compare_line['compare_line_id'])        
        
        # labels for x-axis
        data['x_scale_values'] = list()
        data['x_scale_labels'] = list()
        
        data['show_stop_light'] = self._show_stop_light
        data['show_std_deviation'] = self._show_std_deviation
        
        return data 
    
    def create_headers(self, data, orig_headers, x_scale_values):
        """
        create dict with original (datetime) header values and float values of header
        """
        for measurement_time in orig_headers:
            # set original header dates value
            data['orig_header'].append(measurement_time)
            # set formatted header dates values
            
            formatted_date = self.formatter.format_date(measurement_time)
            data['header'].append(formatted_date)
        
        if orig_headers:
            f_date = orig_headers[0]
            data['x_axis'] = list(float(repr(mktime(ddate.timetuple()) - mktime(f_date.timetuple()))) for ddate in data['orig_header'])
            data['even_header'] = range(len(data['header']))

            first_date = orig_headers[0] 
            last_date = orig_headers[-1]
            data['x_scale_values'] = filter(lambda x_scale_value: x_scale_value <= last_date and x_scale_value >= first_date, x_scale_values)
            
            for x_scale_value in data['x_scale_values']:
                data['x_scale_labels'].append(self.formatter.format_date(x_scale_value))            
        return data
    
    
    def unlock(self):
        if self._filelocker:
            self._filelocker.release()
        self._filelocker = None
    
    def set_logger(self, logger):
        self._logger = logger

    def process_chart_interval(self, charting_interval):
        pass

    def update(self, segment_value_id, charting_interval_id):
        """
        main class for generation metric
        """
        self.chart_gen = ChartGenerator(self.formatter)

        #chart only index interval
        if self.index_interval_only:
            charting_interval_id = self.index_interval_id

        #list of segments or zero for non-segmented
        segments = list()
        if segment_value_id and self._segment and any(segment['segment_value_id'] == segment_value_id for segment in self._segment_values):
            segments = list(segment for segment in self._segment_values if segment['segment_value_id']==segment_value_id)
        elif self._segment_values:
            segments = self._segment_values
        else:
            segments.append(0)

        for segment_value in segments:
            if segment_value:
                self._segment_value_id = segment_value['segment_value_id']
                self._segment_value = segment_value
            else:
                self._segment_value_id = 0
                self._segment_value = None

            self._jfile.set_segment_value(segment_value)
            self._jfile.set_data(self._data)


            self._filelocker = FileLock("%s%s/run_segment_%s" % (self._path, self._id, self._segment_value_id), 0, 0)

            # try to lock run segment file lock
            if not self._filelocker.acquire():
                # if segment file is lock continue for next segment
                if self._logger:
                    self._logger.info("Segment %s is locked. Skip it." % self._segment_value_id)
                continue



            # charting interval is specified and exists
            charting_intervals = []
            if charting_interval_id and self._charting_intervals:
                # get charting interval
                charting_intervals = filter(lambda charting_int: charting_int['charting_interval_id'] == charting_interval_id, self._charting_intervals)

            if not charting_intervals:
                charting_intervals = self._charting_intervals
            self.get_min_max_ever_from_db()
            # get every charting interval
            for charting_interval in charting_intervals:
                self.process_chart_interval(charting_interval)
            # create main meta json

            self.create_meta()
            # release run segment file lock
            self.unlock()


    def index_annotations(self, all_annotations):
        """
        sort range/point annotations by date, type and metric order and add indexes
        """
        all_annotations.sort(key=itemgetter('metric_order'))
        all_annotations.sort(key=itemgetter('is_range'))
        all_annotations.sort(key=itemgetter('time'))
        index = 0
        for i, annotation in enumerate(all_annotations):
            index += 1
            all_annotations[i]['index'] = index
        return all_annotations


class MetricElement(AbstractMetricElement):
    """
    Single metric dashboard element
    """
    min_max_ever =  dict()

    # needed for storing settings for multimetric charting. used only by parent multimetric
    sub_settings = dict()
    
    measured_values = list()
    annotations = list()
    range_annotations = list()
    all_annotations = list()
    
    def __init__(self, id, index_interval_only):
        AbstractMetricElement.__init__(self, id, index_interval_only)
        self._type = 'metric'
        self._path = self.config.metric_root

    def init(self):
        """
        init single metric and set value type (int/float)
        """
        AbstractMetricElement.init(self)
        self.metric_value_type = 'int'
        if self._data['metric_value_type'] == 'float':
            self.metric_value_type = 'float'
    
    def _get_element(self):
        """
        get single metric specific data
        """
        data = AbstractMetricElement._get_element(self)
        data['axis_number'] = 1
        data['line_type'] = 'solid'
        
        #if not data.has_key('chart_layout_id') or not data['chart_layout_id']:
        if 'chart_layout_id' not in data or not data['chart_layout_id']:
            raise Exception("chart_layout_id for %s (%s) is missing" % (data['name'], self._id))
        
        self._db.Query("""SELECT
                                metric_line_color,
                                metric_bar_color,
                                line_width,
                                metric_moving_average_line_color,
                                moving_average_line_width
                            FROM chart_layout
                            WHERE layout_id = %s
                        """, (data['chart_layout_id']))
        data_chart = self._db.record[0]
        if data['metric_chart_display_type'] == 'line':
            data['metric_color'] = data_chart['metric_line_color']
        else:
            data['metric_color'] = data_chart['metric_bar_color']
        data['line_width'] = data_chart['line_width']
        
        if data['metric_moving_average_interval']:
            data['metric_moving_average_label'] = 'Last %s Moving Average'%(data['metric_moving_average_interval'])
            self._show_moving_average = True
        
        if self._show_moving_average or (data['alert_prior_measurement_value_count'] and data['interval_unit']):
            data['metric_std_deviation_moving_average_label'] = 'Last %s %s Moving Average'%(data['alert_prior_measurement_value_count'], data['interval_unit'])
            data['metric_std_deviation_moving_average_interval'] = '%s %s'%(data['alert_prior_measurement_value_count'], data['interval_unit'])
            if not data['metric_unusual_value_std_dev']:
                data['metric_unusual_value_std_dev'] = 2
            self._show_std_deviation = True
        
        data['moving_average_line_color'] = data_chart['metric_moving_average_line_color']
        data['moving_average_line_width'] = data_chart['moving_average_line_width']
        data['moving_average_line_style'] = data['metric_chart_line_style']
        data['moving_average_line_type'] = 'solid' 
        return data
        
    def filter_min_max_to_chart(self, value):
        """
        filter min and max value permitted for chart
        """
        if value is not None:
            if self._data['metric_min_value_to_chart'] is not None and value <= self._data['metric_min_value_to_chart']:
                value = None
            elif self._data['metric_max_value_to_chart'] is not None and value >= self._data['metric_max_value_to_chart']:
                value = None
        return value
    
    def fetch_orig_headers(self):
        """
        get unformatted (raw) values from fetched measurement times
        """
        orig_headers = [measured_value['measurement_time'] for measured_value in self.measured_values]
        return orig_headers
    
    
    #def prepare_measured_values(self, data, end_date, start_date):
    def prepare_measured_values(self, data):
        """
        create dict with data for passing to chart generator
        """
        data[self._id] = dict()
        
        # get min/max values for interval
        data[self._id]['min_for_interval'] = self.get_min_for_interval()
        data[self._id]['max_for_interval'] = self.get_max_for_interval()        
        
        data[self._id]['expired_date'] = self._expired_date
        
        data['rows'][self._id] = OrderedDict()
        

        data['rows'][self._id]['data_settings'] = dict()
        data['rows'][self._id]['data_settings']['label'] = self._data['name']
        data['rows'][self._id]['data_settings']['data'] = list()
        data['rows'][self._id]['data_settings']['axis_number'] = self._data['axis_number']
        data['rows'][self._id]['data_settings']['display_type'] = self._data['metric_chart_display_type']
        data['rows'][self._id]['data_settings']['line_type'] = self._data['line_type']
        data['rows'][self._id]['data_settings']['line_style'] = self._data['metric_chart_line_style']
        data['rows'][self._id]['data_settings']['color'] = FontManager.get_db_color(self._data['metric_color'])
        data['rows'][self._id]['data_settings']['line_width'] = self._data['line_width']
        
        data['rows'][self._id]['min_ever_settings'] = None
        data['rows'][self._id]['max_ever_settings'] = None
        data['rows'][self._id]['average_settings'] = None
        data['rows'][self._id]['stop_light'] = None
        data['rows'][self._id]['compare_settings'] = None
        avg_interval = ''

        #if self._data['metric_moving_average_interval']:
        if self._show_moving_average or self._show_std_deviation:
            data['rows'][self._id]['average_settings'] = dict()

            data['rows'][self._id]['average_settings']['show_moving_average'] = self._show_moving_average
            data['rows'][self._id]['average_settings']['show_std_deviation'] = self._show_std_deviation


            if self._show_moving_average:
                data['rows'][self._id]['average_settings']['label'] = self._data['metric_moving_average_label'] #'%s moving average'%(self._data['metric_moving_average_interval'])
                avg_interval = self._data['metric_moving_average_interval']
            elif self._show_std_deviation:
                data['rows'][self._id]['average_settings']['label'] = self._data['metric_std_deviation_moving_average_label'] #'%s moving average'%(self._data['metric_moving_average_interval'])
                avg_interval = self._data['metric_std_deviation_moving_average_interval']
                
            data['rows'][self._id]['average_settings']['std_deviation_label'] = ''

            data['rows'][self._id]['average_settings']['metric_unusual_value_std_dev'] = 0

            if self._show_std_deviation:
                data['rows'][self._id]['average_settings']['metric_unusual_value_std_dev'] = self._data['metric_unusual_value_std_dev']
                if self._data['metric_unusual_value_std_dev'] == 1:
                    data['rows'][self._id]['average_settings']['std_deviation_label'] = '1 standard deviation'
                else:
                    data['rows'][self._id]['average_settings']['std_deviation_label'] = '%s standard deviations' % self._data['metric_unusual_value_std_dev']

                

            data['rows'][self._id]['average_settings']['data'] = list()
            data['rows'][self._id]['average_settings']['std_deviation_data'] = list()
            data['rows'][self._id]['average_settings']['axis_number'] = self._data['axis_number']
            data['rows'][self._id]['average_settings']['display_type'] = 'line'
            data['rows'][self._id]['average_settings']['color'] = FontManager.get_db_color(self._data['moving_average_line_color'])#int('0x' + self._data['moving_average_line_color'][1:], 16)
            data['rows'][self._id]['average_settings']['line_type'] = self._data['moving_average_line_type']
            data['rows'][self._id]['average_settings']['line_width'] = self._data['moving_average_line_width']
            data['rows'][self._id]['average_settings']['line_style'] = self._data['moving_average_line_style']

        
        
        
        if self._show_stop_light:
            data['rows'][self._id]['stop_light'] = dict()
            data['rows'][self._id]['stop_light']['display_type'] = 'area'
            data['rows'][self._id]['stop_light']['color'] = 0
            data['rows'][self._id]['stop_light']['metric_more_is_better_ind'] = self._data['metric_more_is_better_ind']
            data['rows'][self._id]['stop_light']['good'] = list()
            data['rows'][self._id]['stop_light']['bad'] = list()
            data['rows'][self._id]['stop_light']['deviation'] = list()
        
        for measured_value in self.measured_values:
            # pre format measured value
            value = self.formatter.pre_format(measured_value['measurement_value'], self._data['display_mask_id'])
            data['rows'][self._id]['data_settings']['data'].append(value)
            
            # stop light values
            if self._show_stop_light:
                bad_value = self.formatter.pre_format(measured_value['stoplight_bad_threshold_value'], self._data['display_mask_id'])
                data['rows'][self._id]['stop_light']['bad'].append(bad_value)
                good_value = self.formatter.pre_format(measured_value['stoplight_good_threshold_value'], self._data['display_mask_id'])
                data['rows'][self._id]['stop_light']['good'].append(good_value)
                deviation_value = self.formatter.pre_format(measured_value['standard_deviation_value'], self._data['display_mask_id'])
                data['rows'][self._id]['stop_light']['deviation'].append(deviation_value)
                
            #get moving average line
            #if self._data['metric_moving_average_interval']:
            if self._show_moving_average or self._show_std_deviation:
                # draw moving average line only up to expired date 
                if self._expired_date and measured_value['measurement_time'] > self._expired_date:
                    value = None
                    std_deviation_value = None
                else:
                    if measured_value['moving_average_value'] is None:
                        # calculate average
                        aver_value = self.filter_min_max_to_chart(self._calc_average_value(measured_value['measurement_time'], avg_interval))
                        std_deviation_value = None
                    else:
                        # take average from db
                        aver_value = self.filter_min_max_to_chart(measured_value['moving_average_value'])
                        std_deviation_value = measured_value['standard_deviation_value']
                    #value = self.formatter.format_orig(aver_value, self._data['display_mask_id'])
                    value = self.formatter.pre_format(aver_value, self._data['display_mask_id'])
                    std_deviation_value = self.formatter.pre_format(std_deviation_value, self._data['display_mask_id'])
                data['rows'][self._id]['average_settings']['data'].append(value)
                data['rows'][self._id]['average_settings']['std_deviation_data'].append(std_deviation_value)
        
        # set min/max ever line
        data['rows'][self._id]['min_ever_settings'] = self.get_ever_min_line_for_chart()
        data['rows'][self._id]['max_ever_settings'] = self.get_ever_max_line_for_chart()

        return data

    
    def fetch_interval_values(self, end_date, start_date):
        """
        get measured values for selected time interval
        """
        self.measured_values = list()
        self._db.Query("""SELECT
                        measurement_time,
                        moving_average_value,
                        metric_measured_value_id,
                        metric_id,
                        measurement_value_"""+self.metric_value_type+""" AS measurement_value,
                        standard_deviation_value,
                        stoplight_bad_threshold_value,
                        stoplight_good_threshold_value
                    FROM metric_measured_value
                WHERE
                    metric_id = %s
                    AND metric_measured_value.measurement_time <= %s
                    AND metric_measured_value.measurement_time >= %s
                    AND segment_value_id = %s
                ORDER BY measurement_time
                """,( self._data['element_id'], str(end_date), str(start_date), self._segment_value_id))

        for measured_value in self._db.record:
            # filter min/max value to chart
            measured_value['measurement_value'] = self.filter_min_max_to_chart(measured_value['measurement_value'])
            # format value
            measured_value['formatted_measurement_value'] = self.formatter.format_full(measured_value['measurement_value'], self._data['display_mask_id'])
            # format good/bad stop light values
            #measured_value['formatted_standard_deviation_value'] = self.formatter.format_full(measured_value['standard_deviation_value'], self._data['display_mask_id'])
            measured_value['formatted_stoplight_bad_threshold_value'] = self.formatter.format_full(measured_value['stoplight_bad_threshold_value'], self._data['display_mask_id'])
            measured_value['formatted_stoplight_good_threshold_value'] = self.formatter.format_full(measured_value['stoplight_good_threshold_value'], self._data['display_mask_id'])
            self.measured_values.append(measured_value)
            
         

    def _get_x_title(self):
        """
        get x-axis title
        """
        return self._data['chart_x_axis_label']

    def _get_y_title(self, charting_interval):
        """
        get y-axis title
        """
        #if charting_interval and charting_interval.has_key('chart_title_display_suffix'):
        if charting_interval and 'chart_title_display_suffix' in charting_interval:
            return "%s  - %s" % (self._data['name'], charting_interval['chart_title_display_suffix'])
            #return self._data['name'] + ' - ' + charting_interval['chart_title_display_suffix']
        return self._data['name']

    def get_compare_line_values(self, compare_line, dates):
        """
        get measured values for selected compare line
        """
        compare_line_dict = dict()
        compare_line_dict['label'] = compare_line['line_display_name']
        compare_line_dict['data'] = list()
        compare_line_dict['line_width'] = compare_line['line_width']
        compare_line_dict['line_color'] = compare_line['line_color']
        compare_line_dict['line_type'] = compare_line['line_type']
        for date in dates:
            res = self._db.Query("""SELECT metric_measured_value.*,
                            measurement_value_"""+self.metric_value_type+""" AS measurement_value
                        FROM metric_measured_value
                    WHERE
                        metric_id = %s
                    AND metric_measured_value.measurement_time = DATE_SUB(%s, INTERVAL """ + str(compare_line['compare_interval_value'])+' ' + str(compare_line['compare_interval_unit']) +""")
                    AND segment_value_id = %s
                    """,
                    (self._data['element_id'], date, self._segment_value_id))
            if res:
                row = self._db.record[0]
                row['measurement_value'] = self.filter_min_max_to_chart(row['measurement_value'])
                #value = self.formatter.format_orig(row['measurement_value'], self._data['display_mask_id'])
                value = self.formatter.pre_format(row['measurement_value'], self._data['display_mask_id'])
                compare_line_dict['data'].append(value)
            else:
                compare_line_dict['data'].append(None)
        return compare_line_dict

    def get_y_axis_format(self):
        """
        get left y axis format
        """
        return self._get_y_axis_format(self._data['display_mask_id'])

    def spread_to_expired_date(self, start_date, x_scale_values):
        if self.measured_values:
            # period from last measurement value till end date is expired
            
            #get the latest measurement time with non null value
            self._expired_date = last_meas_time = self.measured_values[-1]['measurement_time']
            for i in reversed(self.measured_values):
                if i['measurement_value'] is None:
                    self._expired_date = i['measurement_time']
                else:
                    break
            
        else:
            #whole period is expired
            self._expired_date = last_meas_time = start_date
            #add first date of period
            self.measured_values.append({'formatted_measurement_value': '', 
                                          'metric_id':self._id,
                                          'metric_measured_value_id':0,
                                          'measurement_value': None,
                                          'measurement_time': start_date,
                                          'moving_average_value': None,
                                          'standard_deviation_value': None,
                                          'stoplight_bad_threshold_value': None,
                                          'stoplight_good_threshold_value': None
                                          })
        
        for x_scale_date in x_scale_values:
            if x_scale_date > last_meas_time:
                self.measured_values.append({'formatted_measurement_value': '', 
                                          'metric_id':self._id,
                                          'metric_measured_value_id':0,
                                          'measurement_value': None,
                                          'measurement_time': x_scale_date,
                                          'moving_average_value': None,
                                          'standard_deviation_value': None,
                                          'stoplight_bad_threshold_value': None,
                                          'stoplight_good_threshold_value': None
                                          })
#        """
#        #next_date = last_meas_time + datetime.timedelta(seconds = self._data['max_time_before_expired_sec'])
#
#        # add empty values till expired date
#        while next_date < end_date:
#            if not any(row['measurement_time'] == next_date for row in self.measured_values):
#                self.measured_values.append({'formatted_measurement_value': '',
#                                          'metric_id':self._id,
#                                          'metric_measured_value_id':0,
#                                          'measurement_value': None,
#                                          'measurement_time': next_date,
#                                          'moving_average_value': None})
#            next_date = next_date + datetime.timedelta(seconds = self._data['max_time_before_expired_sec'])
#
#        #add last date of period
#        self.measured_values.append({'formatted_measurement_value': '',
#                                          'metric_id':self._id,
#                                          'metric_measured_value_id':0,
#                                          'measurement_value': None,
#                                          'measurement_time': end_date,
#                                          'moving_average_value': None})
#        """
    def get_all_annotations(self, header, orig_header, values, metric_order):
        """
        get range and point annotations
        """
        all_annotations = list()
        # get point annotations list
        point_annotations = self._get_annotations(header)

        # get range annotations list
        range_annotations = self._get_range_annotations(orig_header, values)

        # add to single list
        for i in point_annotations.keys():
            all_annotations.append({'time': orig_header[i],
                                    'is_range': 0, 
                                    'index': None,
                                    'header_index': i,
                                    'metric_id': self._id,
                                    'metric_order': metric_order,
                                    'data': point_annotations[i]})
        # add to single list
        for i, range_annotation in enumerate(range_annotations):
            all_annotations.append({'time': range_annotation['annotation_measurement_start_time'],
                                    'is_range': 1,
                                    'index': None,
                                    'header_index': None,
                                    'metric_id': self._id,
                                    'metric_order': metric_order,
                                    'data': range_annotation})

        return all_annotations
    
    def parse_annotations(self, all_annotations, header_len):
        """
        divide all annotations list into range/point annotations lists
        """
        self.annotations = list()
        self.range_annotations = list()
        point_annotations = [False for i in xrange(header_len + 1)]
        for annotation in all_annotations:
            if self._id == annotation['metric_id']:
                if annotation['is_range']:
                    annotation['data']['index'] = annotation['index']
                    self.range_annotations.append(annotation['data'])
                else:
                    point_annotations[annotation['header_index']] = annotation['index']
                    for point_annotation in annotation['data']:
                        point_annotation['index'] = annotation['index']
                        self.annotations.append(point_annotation)

        return point_annotations, self.range_annotations

    def process_interval(self, charting_interval, end_date, start_date, x_scale_values):
        """
        process selected charting interval data
        """

        # fetching measured values
        self.fetch_interval_values(end_date, start_date)

        # if metric is expired
        if self._expired_date and (not self.measured_values or self._expired_date > self.measured_values[-1]['measurement_time']):
            self.spread_to_expired_date(start_date, x_scale_values)
        else:
            self._expired_date = None

        # get list of compare lines
        compare_lines = self.fetch_compare_lines(charting_interval)
        #create empty data dict
        data = self.init_charting_data(compare_lines)
        data['thin_by_metric_id'] = self._id
        
        data['expired_date'] = self._expired_date
        
        
        #get original dates
        orig_headers = self.fetch_orig_headers()
        
        #create formatted header  
        data = self.create_headers(data, orig_headers, x_scale_values)
        
        # create dict for passing to chart
        #data = self.prepare_measured_values(data, end_date, start_date)
        data = self.prepare_measured_values(data)

        data['primary_axis_display_mask'] = self.get_y_axis_format()

        # get all annotations
        all_annotations = self.get_all_annotations(data['header'], data['orig_header'], data['rows'][self._id]['data_settings']['data'], 0)
        # index annotations
        indexed_all_annotations = self.index_annotations(all_annotations)
        # divide all annotations into point/range lists
        data['annotations'][self._id], data['range_annotations'][self._id] = self.parse_annotations(indexed_all_annotations, len(data['header']))

        for compare_line in compare_lines:
            data['rows'][self._id]['compare_settings_%s'%(compare_line['compare_line_id'])] = self.get_compare_line_values(compare_line, data['orig_header'])

        # set titles
        data['x_axis_title'] = self._get_x_title()
        data['y_axis_title_left'] = self._get_y_title(charting_interval)
        data['y_axis_num'] = 1

        return data

    def _calc_average_value(self, date, interval):
        """
        fetch and calc value for specified average line date
        """
        if not interval:
            return None
        average = None
        self._db.Query("""SELECT metric_measured_value.*,
                        measurement_value_"""+self.metric_value_type+""" AS measurement_value
                    FROM metric_measured_value
                WHERE
                    metric_id = %s
                AND metric_measured_value.measurement_time <= %s
                AND metric_measured_value.measurement_time >= DATE_SUB(%s, INTERVAL """ + interval + """)
                AND segment_value_id = %s""",
                (self._data['element_id'], date, date, self._segment_value_id ))

        averages = list(row['measurement_value'] for row in self._db.record)
        if averages:
            average = float(sum(averages)) / len(averages)
        return average

    
    def get_min_max_ever_from_db(self):
        """
        get minimum/maximum ever dict
        """
        res = self._db.Query("""SELECT 
                        metric_max_value_time_formatted,
                        metric_min_value_time_formatted,
                        metric_max_value_time,
                        metric_min_value_time,
                        metric_max_value_formatted,
                        metric_min_value_formatted,
                        metric_max_value_""" + self.metric_value_type + """ AS metric_max_value,
                        metric_min_value_""" + self.metric_value_type + """ AS metric_min_value
                    FROM last_dashboard_element_segment_value
                WHERE
                    element_id = %s
                AND segment_value_id = %s""",
                (self._data['element_id'], self._segment_value_id ))
        if res:
            self.min_max_ever = self._db.record[0]

            # min/max value to chart
            min_max_condition = ''
            if self._data['metric_min_value_to_chart'] is not None:
                min_max_condition += ' AND measurement_value_'+self.metric_value_type+'>='+self._db.escape_string(repr(self._data['metric_min_value_to_chart']))
            if self._data['metric_max_value_to_chart'] is not None:
                min_max_condition += ' AND measurement_value_'+self.metric_value_type+'<='+self._db.escape_string(repr(self._data['metric_max_value_to_chart']))
            
            # let's check, if max/min values are correct (they may be deleted)
            # check max ever
            if self.min_max_ever['metric_max_value'] is not None:
                res = self._db.Query("""SELECT measurement_value_"""+self.metric_value_type+""" AS measurement_value
                                            FROM metric_measured_value
                                        WHERE 1
                                            AND metric_id = %s
                                            AND segment_value_id = %s
                                            AND measurement_time = %s
                                        LIMIT 0, 1
                                        """,(self._data['element_id'], self._segment_value_id, self.min_max_ever['metric_max_value_time']))
                if res:
                    max_ever = self._db.record[0]
                    # check if max measured value is still there 
                    if max_ever['measurement_value'] != self.min_max_ever['metric_max_value']:
                        self.min_max_ever['metric_max_value'] = None
                else:
                    self.min_max_ever['metric_max_value'] = None
            
            # check min ever
            if self.min_max_ever['metric_min_value'] is not None:
                res = self._db.Query("""SELECT measurement_value_"""+self.metric_value_type+""" AS measurement_value
                                            FROM metric_measured_value
                                        WHERE 1
                                            AND metric_id = %s
                                            AND segment_value_id = %s
                                            AND measurement_time = %s
                                        LIMIT 0, 1
                                        """,(self._data['element_id'], self._segment_value_id, self.min_max_ever['metric_min_value_time']))
                if res:
                    min_ever = self._db.record[0]
                    # check if min measured value is still there
                    if min_ever['measurement_value'] != self.min_max_ever['metric_min_value']:
                        self.min_max_ever['metric_min_value'] = None
                else:
                    self.min_max_ever['metric_min_value'] = None
            
            # filter values
            self.min_max_ever['metric_max_value'] = self.filter_min_max_to_chart(self.min_max_ever['metric_max_value'])
            self.min_max_ever['metric_min_value'] = self.filter_min_max_to_chart(self.min_max_ever['metric_min_value'])
            
            # if there is no max ever, let's create it 
            if self.min_max_ever['metric_max_value'] is None:
                res = self._db.Query("""SELECT measurement_value_"""+self.metric_value_type+""" AS measurement_value,
                                                measurement_time
                                        FROM metric_measured_value
                                            WHERE 1
                                            """ + min_max_condition + """
                                            AND metric_id = %s
                                            AND segment_value_id = %s
                                        ORDER BY measurement_value DESC
                                        LIMIT 0, 1
                                        """,(self._data['element_id'], self._segment_value_id))
                if res:
                    max_ever = self._db.record[0]
                    max_ever['formatted_measurement_value'] = self.formatter.format_full(max_ever['measurement_value'], self._data['display_mask_id'])
                    self.update_min_max_ever('max', max_ever)

            # if there is no min ever, let's create it 
            if self.min_max_ever['metric_min_value'] is None:
                res = self._db.Query("""SELECT measurement_value_"""+self.metric_value_type+""" AS measurement_value,
                                                measurement_time
                                        FROM metric_measured_value
                                            WHERE 1
                                            """ + min_max_condition + """
                                            AND metric_id = %s
                                            AND segment_value_id = %s
                                        ORDER BY measurement_value ASC
                                        LIMIT 0, 1
                                        """,(self._data['element_id'], self._segment_value_id))
                if res:
                    min_ever = self._db.record[0]
                    min_ever['formatted_measurement_value'] = self.formatter.format_full(min_ever['measurement_value'], self._data['display_mask_id'])
                    self.update_min_max_ever('min', min_ever)

             
        else:
            self.min_max_ever = dict()
            self.min_max_ever['metric_max_value'] = None
            self.min_max_ever['metric_min_value'] = None
            

    
    def get_ever_min_line_for_chart(self):
        """
        return min ever if it should be shown in chart
        """
        ever_value = None
        if self.min_max_ever['metric_min_value'] is not None  and self._data['metric_show_min_ever_on_chart_ind'] == 'Y':
            ever_value = {'label':'Minimum Ever', 'data':list()}
            ever_value['data'].append(self.min_max_ever['metric_min_value'])
        return ever_value

    def get_ever_max_line_for_chart(self):
        """
        return max ever if it should be shown in chart
        """
        ever_value = None
        if self.min_max_ever['metric_max_value'] is not None and self._data['metric_show_max_ever_on_chart_ind'] == 'Y':
            ever_value = {'label':'Maximum Ever', 'data':list()}
            ever_value['data'].append(self.min_max_ever['metric_max_value'])
        return ever_value
    
    
    def get_min_for_interval(self):
        """
        get min values in current charting interval dates
        """
        if self.measured_values:
            non_null_values = [measured_value for measured_value in self.measured_values if measured_value['measurement_value'] is not None]
            if non_null_values:
                value = min(non_null_values, key = itemgetter('measurement_value'))
                if self.min_max_ever['metric_min_value'] is None or (self.min_max_ever['metric_min_value'] is not None and value['measurement_value'] < self.min_max_ever['metric_min_value']):
                    self.update_min_max_ever('min', value) 
                
                return {'value': value['measurement_value'], 'measurement_time': value['measurement_time']}
        return None

    def get_max_for_interval(self):
        """
        get max values in current charting interval dates
        """
        if self.measured_values:
            non_null_values = [measured_value for measured_value in self.measured_values if measured_value['measurement_value'] is not None]
            if non_null_values:
                value = max(non_null_values, key = itemgetter('measurement_value'))
                if self.min_max_ever['metric_max_value'] is None or (self.min_max_ever['metric_max_value'] is not None and value['measurement_value'] > self.min_max_ever['metric_max_value']):
                    self.update_min_max_ever('max', value)
                
                return {'value': value['measurement_value'], 'measurement_time': value['measurement_time']}
        return None

    def update_min_max_ever(self, min_max, value):
        value['formatted_measurement_time'] = self.formatter.format_date(value['measurement_time'])
        
        self._db.Query("""UPDATE last_dashboard_element_segment_value 
                            SET metric_""" + min_max + """_value_""" + self.metric_value_type+ """ = %s,
                            metric_""" + min_max + """_value_formatted = %s,
                            metric_""" + min_max + """_value_time = %s,
                            metric_""" + min_max + """_value_time_formatted = %s  
                            
                            WHERE element_id = %s
                                AND segment_value_id = %s""", (
                                                               value['measurement_value'], 
                                                               value['formatted_measurement_value'], 
                                                               value['measurement_time'],
                                                               value['formatted_measurement_time'],
                                                               self._id,
                                                               self._segment_value_id))
        self.min_max_ever['metric_' + min_max + '_value'] = value['measurement_value']
        self.min_max_ever['metric_' + min_max + '_value_formatted'] = value['formatted_measurement_value']
        self.min_max_ever['metric_' + min_max + '_value_time'] = value['measurement_time']
        self.min_max_ever['metric_' + min_max + '_value_time_formatted'] = value['formatted_measurement_time']


    
    def _get_curr_value(self):
        """
        get metric current measured value
        """
        value = None
        #measurement_time = None

        # min/max value to chart
        min_max_condition = ''
        if self._data['metric_min_value_to_chart'] is not None:
            min_max_condition += ' AND measurement_value_'+self.metric_value_type+'>='+self._db.escape_string(repr(self._data['metric_min_value_to_chart']))
        if self._data['metric_max_value_to_chart'] is not None:
            min_max_condition += ' AND measurement_value_'+self.metric_value_type+'<='+self._db.escape_string(repr(self._data['metric_max_value_to_chart']))

        res = self._db.Query("""SELECT metric_measured_value.*,
                        measurement_value_"""+self.metric_value_type+""" AS measurement_value
                    FROM metric_measured_value
                WHERE 1
                    """ + min_max_condition + """
                    AND metric_id = %s
                    AND measurement_value_"""+self.metric_value_type+""" IS NOT NULL
                    AND segment_value_id = %s
                ORDER BY measurement_time DESC
                LIMIT 0, 1
                """,(self._data['element_id'], self._segment_value_id))

        if res:
            extreme_value = self._db.record[0]
            value = extreme_value['measurement_value']
            #measurement_time = extreme_value['measurement_time']
            #value = self.formatter.format_orig(value, self._data['display_mask_id'])
            value = self.formatter.pre_format(value, self._data['display_mask_id'])

        return value
    def prepare_min_max(self, metric_value):
        if self.min_max_ever['metric_min_value'] is not None:
            #metric_value['min_value'] = self.formatter.format_half(self.min_ever['value'], self._data['display_mask_id'])
            #metric_value['min_value'] = self.formatter.format_full(self.min_ever['value'], self._data['display_mask_id'])
            #metric_value['min_reached_on'] = self.formatter.format_date(self.min_ever['measurement_time'])
            metric_value['min_value'] = self.min_max_ever['metric_min_value_formatted']
            metric_value['min_reached_on'] = self.min_max_ever['metric_min_value_time_formatted']
        else:
            metric_value['min_value'] = ''
            metric_value['min_reached_on'] = ''
        if self.min_max_ever['metric_max_value'] is not None:
            #metric_value['max_value'] = self.formatter.format_half(self.max_ever['value'], self._data['display_mask_id'])
            #metric_value['max_value'] = self.formatter.format_full(self.max_ever['value'], self._data['display_mask_id'])
            #metric_value['max_reached_on'] = self.formatter.format_date(self.max_ever['measurement_time'])
            metric_value['max_value'] = self.min_max_ever['metric_max_value_formatted']
            metric_value['max_reached_on'] = self.min_max_ever['metric_max_value_time_formatted']
        else:
            metric_value['max_value'] = ''
            metric_value['max_reached_on'] = ''
        return metric_value
    
    def prepare_metric(self):
        """
        create metric dict main meta file
        """
        metrics = list()

        metric_value = dict()
        metric_value['metric_element_id'] = self._data['element_id']
        metric_value['metric_name'] = self._data['name']
        metric_value['metric_descr'] = self._data['description']
        metric_value['metric_dashboard_category'] = self._data['category']
        metric_value['metric_primary_topic'] = self._data['topic_name']
        metric_value['metric_business_owner'] = self._data['business_owner']
        metric_value['metric_tech_owner'] = self._data['technical_owner']
        metric_value['metric_interval_id'] = self._data['measurement_interval_id']
        metric_value['metric_moving_average_interval'] = self._data['metric_moving_average_interval']
        #metric_value['curr_value'] = self.formatter.format_half(self._get_curr_value(), self._data['display_mask_id'])
        metric_value['curr_value'] = self.formatter.format_full(self._get_curr_value(), self._data['display_mask_id'])
        
        metric_value = self.prepare_min_max(metric_value)

        

        metric_value['compare_to'] = ''

        metrics.append(metric_value)
        return metrics

    def _get_range_annotations(self, header, values):
        range_annotations = list()

        start_time = header[0]
        finish_time = header[-1]
        self._db.Query("""
                            SELECT user_annotation.*,
                                     user.username
                                FROM user_annotation,  user
                                WHERE
                                    user.user_id = user_annotation.user_id
                                    AND user_annotation.annotation_interval = 'range'
                                    AND element_id = %s
                                    AND segment_value_id = %s
                                    AND annotation_measurement_start_time <= %s
                                    AND annotation_measurement_finish_time >= %s
                                ORDER BY annotation_time
                                """, (self._id, self._segment_value_id, finish_time, start_time))


        for ann in self._db.record:
            ann['metric_id'] = self._id
            ann['measurement_value'] = ''
            ann['left_marker'] = True
            ann['right_marker'] = True
            ann['from_time'] = ann['annotation_measurement_start_time']
            ann['to_time'] = ann['annotation_measurement_finish_time']

            if ann['annotation_measurement_start_time'] < start_time:
                ann['left_marker'] = False
                ann['from_time'] = start_time



            if ann['annotation_measurement_finish_time'] > finish_time:
                ann['right_marker'] = False
                ann['to_time'] = finish_time

            from_ind = 0
            for i, d in enumerate(header):
                if d and ann['from_time'] <= d:
                    from_ind = i
                    break
            to_ind = len(header) - 1
            for i, d in enumerate(header):
                to_ind = i
                if d and ann['to_time'] < d:
                    break
            value = max(values[from_ind:to_ind])
            if value is None:
                value = max(values)
            if not value is None:
                #annotation_index += 1
                ann['index'] = None
                ann['value'] = value
                range_annotations.append(ann)
        return range_annotations

    def _get_annotations(self, header):
        """
        get annotations for specified period
        """
        # annotations list

        # metric measured values list with annotation index
        #metric_annotations = list()
        metric_annotations = dict()
        ids = list()
        real_values = dict()
        
        for measured_value in self.measured_values:
            if measured_value['metric_measured_value_id']:
                ids.append(measured_value['metric_measured_value_id'])
                real_values[measured_value['metric_measured_value_id']] = measured_value
        
        format_strings = ','.join(['%s'] * len(ids))
        param = list(ids)
        annotations = dict()
        if ids:
            self._db.Query("""
                            SELECT user_annotation.*,
                                     user.username,
                                     metric_annotation.metric_instance_id
                                FROM user_annotation, metric_annotation, user
                                WHERE
                                    user_annotation.user_annotation_id = metric_annotation.user_annotation_id
                                    AND user.user_id = user_annotation.user_id
                                    AND metric_annotation.metric_instance_id IN(%s)
                                    AND user_annotation.annotation_interval = 'point'
                                ORDER BY annotation_time
                                """ % format_strings, tuple(param))
            
            for ann in self._db.record:
                #if not annotations.has_key(ann['metric_instance_id']):
                if ann['metric_instance_id'] not in annotations:
                    annotations[ann['metric_instance_id']] = list()
                annotations[ann['metric_instance_id']].append(ann)

        for i, measured_value in enumerate(self.measured_values):
            #if annotations.has_key(measured_value['metric_measured_value_id']):
            if measured_value['metric_measured_value_id'] in annotations:
                for j, ann in enumerate(annotations[measured_value['metric_measured_value_id']]):
                    annotations[measured_value['metric_measured_value_id']][j]['measurement_value'] = measured_value['formatted_measurement_value']
                    annotations[measured_value['metric_measured_value_id']][j]['metric_id'] = self._id
                    annotations[measured_value['metric_measured_value_id']][j]['index'] = None
                    annotations[measured_value['metric_measured_value_id']][j]['meas_index'] = header[i]
                metric_annotations[i] = annotations[measured_value['metric_measured_value_id']]

        return metric_annotations

    def _prepare_map(self, map, prepared_data):
        """
        parse map data received from chart generator
        """
        prepared_map = list()
        #if not map['data'].has_key(self._id):
        if self._id not in map['data']:
            return prepared_map

        for i, date in enumerate(prepared_data['header']):
            prepared_map_item = OrderedDict()

            # get measured value coordinates
            #if map['data'][self._id].has_key(i):
            if i in map['data'][self._id]:
                row = map['data'][self._id][i]
                prepared_map_item['shape'] = row['shape']
                prepared_map_item['coords'] = row['coords']
                prepared_map_item['metric_id'] = self._id
                prepared_map_item['metric_name'] = self._data['name']
                prepared_map_item['metric_instance_id'] = self.measured_values[i]['metric_measured_value_id']
                prepared_map_item['meas_time'] = date
                prepared_map_item['raw_meas_time'] = str(self.measured_values[i]['measurement_time'])
                #prepared_map_item['value'] = self.formatter.format_half(prepared_data['rows'][self._id]['data_settings']['data'][i], self._data['display_mask_id'])s
                prepared_map_item['value'] = self.measured_values[i]['formatted_measurement_value']
                prepared_map_item['raw_value'] = self.measured_values[i]['measurement_value']
                prepared_map_item['moving_avg_value'] = ''
                prepared_map_item['raw_moving_avg_value'] = ''
                prepared_map_item['standard_deviation_value'] = ''
                prepared_map_item['raw_standard_deviation_value'] = ''
                #if self._data['metric_moving_average_interval']:
                if self._show_std_deviation or self._show_moving_average:
                    #prepared_map_item['moving_avg_value'] = self.formatter.format_half(prepared_data['rows'][self._id]['average_settings']['data'][i], self._data['display_mask_id'])
                    prepared_map_item['raw_moving_avg_value'] = prepared_data['rows'][self._id]['average_settings']['data'][i]
                    prepared_map_item['moving_avg_value'] = self.formatter.format_full(prepared_data['rows'][self._id]['average_settings']['data'][i], self._data['display_mask_id'])
                    if self._show_moving_average:
                        prepared_map_item['moving average period'] = self._data['metric_moving_average_interval']
                    else:
                        prepared_map_item['moving average period'] = self._data['metric_std_deviation_moving_average_interval']

                    if self._show_std_deviation:
                        prepared_map_item['raw_standard_deviation_value'] = self.measured_values[i]['standard_deviation_value']
                        prepared_map_item['standard_deviation_value'] = self.formatter.format_full(self.measured_values[i]['standard_deviation_value'], self._data['display_mask_id'])

                prepared_map_item['compares'] = list()

                for compare_line in prepared_data['compare_lines']:
                    compare_id = 'compare_settings_%s' % compare_line
                    compare_row = OrderedDict()
                    compare_row['disp_line_name'] = prepared_data['rows'][self._id][compare_id]['label']
                    compare_row['line_value'] = self.formatter.format_full(prepared_data['rows'][self._id][compare_id]['data'][i], self._data['display_mask_id'])
                    prepared_map_item['compares'].append(compare_row)
                

                prepared_map_item['stoplight_bad_threshold_value'] = ''
                prepared_map_item['stoplight_good_threshold_value'] = ''
                prepared_map_item['stoplight_variance'] = ''

                if self._show_stop_light:
                    prepared_map_item['stoplight_bad_threshold_value'] = self.measured_values[i]['formatted_stoplight_bad_threshold_value']
                    prepared_map_item['stoplight_good_threshold_value'] = self.measured_values[i]['formatted_stoplight_good_threshold_value']
                    prepared_map_item['raw_stoplight_bad_threshold_value'] = self.measured_values[i]['stoplight_bad_threshold_value']
                    prepared_map_item['raw_stoplight_good_threshold_value'] = self.measured_values[i]['stoplight_good_threshold_value']

                    
                    if self._data['metric_more_is_better_ind'] == 'Y':
                        if self.measured_values[i]['measurement_value'] is not None:
                            if self.measured_values[i]['stoplight_good_threshold_value'] is not None and self.measured_values[i]['measurement_value'] >= self.measured_values[i]['stoplight_good_threshold_value']:
                                prepared_map_item['stoplight_variance'] = self.formatter.format_full(abs(self.measured_values[i]['measurement_value'] - self.measured_values[i]['stoplight_good_threshold_value']), self._data['display_mask_id'])
                            elif self.measured_values[i]['stoplight_bad_threshold_value'] is not None and self.measured_values[i]['measurement_value'] <= self.measured_values[i]['stoplight_bad_threshold_value']:
                                prepared_map_item['stoplight_variance'] = self.formatter.format_full(abs(self.measured_values[i]['stoplight_bad_threshold_value'] - self.measured_values[i]['measurement_value']), self._data['display_mask_id'])
                    
                prepared_map.append(prepared_map_item)

        for i, annot in enumerate(self.annotations):
            self.annotations[i]['coords'] = map['annotations'][annot['index']]['coords']
            self.annotations[i]['shape'] = map['annotations'][annot['index']]['shape']
            self.annotations[i]['annotation_interval'] = 'point'
            self.annotations[i]['start_time'] = ''
            self.annotations[i]['finish_time'] = ''
            self.annotations[i]['raw_start_time'] = ''
            self.annotations[i]['raw_finish_time'] = ''
        
        for i, annot in enumerate(self.range_annotations):
            self.range_annotations[i]['meas_index'] = ''
            self.range_annotations[i]['coords'] = map['range_annotations'][annot['index']]['coords']
            self.range_annotations[i]['shape'] = map['range_annotations'][annot['index']]['shape']
            self.range_annotations[i]['annotation_interval'] = 'range'
            self.range_annotations[i]['raw_start_time'] = self.range_annotations[i]['annotation_measurement_start_time']
            self.range_annotations[i]['raw_finish_time'] = self.range_annotations[i]['annotation_measurement_finish_time']

            self.range_annotations[i]['start_time'] = self.formatter.format_date(self.range_annotations[i]['annotation_measurement_start_time'])
            self.range_annotations[i]['finish_time'] = self.formatter.format_date(self.range_annotations[i]['annotation_measurement_finish_time'])
        self.annotations.extend(self.range_annotations)
           
        return prepared_map


    def process_chart_interval(self, charting_interval):
        """
        draw charts for specified charting interval
        """
        self.formatter.set_def_date_format_id(charting_interval['display_mask_id'])
        
        # get period for fetching measured values
        end_date, start_date, scale_values = self.get_interval(charting_interval)
        
        # prepare data for charting
        prepared_data = self.process_interval(charting_interval, end_date, start_date, scale_values)

        # draw chart, get image map data
        orig_map = self.chart_gen.metric_chart(self._id, charting_interval['charting_interval_id'], prepared_data, self._jfile)

        # fetch image map data for json
        map = self._prepare_map(orig_map, prepared_data)

        # write json
        self._jfile.make_chart_interval(map, charting_interval['charting_interval_id'], charting_interval, self.annotations)
        
        #create thumbnail/preview if it's not index interval only mode 
        if (not self.index_interval_only) and charting_interval['index_charting_interval_ind'] == 'Y':
            thumb_prepared_data = copy.deepcopy(prepared_data)
            self.chart_gen.metric_thumbnail(self._id, charting_interval['charting_interval_id'],  thumb_prepared_data, self._jfile)
            self.chart_gen.metric_preview(self._id, charting_interval['charting_interval_id'],  prepared_data, self._jfile)





class MultiMetricElement(AbstractMetricElement):
    """
    Multi metric dashboard element
    """
    _metrics = OrderedDict()
    stoplight_element_id = 0

    def __init__(self, id, index_interval_only):
        AbstractMetricElement.__init__(self, id, index_interval_only)
        self._type = 'multimetric'
        
        # json file operation wrapper
        self._path = self.config.multimetric_root


    def init(self):
        """
        init multi metric and get all sub metrics
        """

        AbstractMetricElement.init(self)
        self._metrics = self._get_metrics()
    
    def _get_element(self):
        """
        get multi metric specific data
        """
        data = AbstractMetricElement._get_element(self)
        
        self._db.Query("""SELECT
                                multi_metric_moving_average_line_type,
                                multi_metric_moving_average_line_width
                            FROM chart_layout
                            WHERE layout_id = %s
                        """, (data['chart_layout_id']))
        data_chart = self._db.record[0]
        
        data['multi_metric_moving_average_line_type'] = data_chart['multi_metric_moving_average_line_type']
        data['multi_metric_moving_average_line_width'] = data_chart['multi_metric_moving_average_line_width']
        self.stoplight_element_id = data['multi_chart_stoplight_metric_element_id']
        
        return data

    def _get_metrics(self):
        """
        return list of single metrics and set specific settings for each metric
        """
        metrics = OrderedDict()
        res = self._db.Query("""SELECT chart_metric.*, dashboard_element.`element_id`
                            FROM chart_metric, dashboard_element
                        WHERE
                            chart_metric.chart_element_id = %s
                        AND
                            dashboard_element.`element_id` = chart_metric.metric_element_id
                        AND
                            IFNULL(dashboard_element.`segment_id`,0) = %s
                        ORDER BY charting_order""",(self._id, self._data['segment_id']))
        if not res:
            raise Exception("No single metrics for multi-metric element %s (%s) available" % (self._data['name'], self._id))
        
        for row in self._db.record:
            metric = MetricElement(row['element_id'], self.index_interval_only)
            metric.sub_settings = row
            metric._data['include_metric_in_thumbnail_ind'] = row['include_metric_in_thumbnail_ind']
            metric._data['measurement_interval_id'] = self._data['measurement_interval_id']
            
            if self.stoplight_element_id and metric._id == self.stoplight_element_id:
                self._data['show_expired_zone_ind'] = metric._data['show_expired_zone_ind']
                self._data['max_time_before_expired_sec'] = metric._data['max_time_before_expired_sec']
            else:
                metric._data['show_expired_zone_ind'] = 'N'
            
            if metric._data['metric_display_label']:
                #metric._data['name'] = "%s %s"%(metric._data['metric_display_label'], metric._data['name'])
                metric._data['name'] = metric._data['metric_display_label']
                        
            if row['include_moving_average_line_ind'] == 'Y':
                res = self._db.Query("""SELECT *
                            FROM chart_layout_dataset
                        WHERE
                            chart_layout_dataset.layout_id = %s
                            AND color = %s
                        """,(self._data['chart_layout_id'], row['metric_color']))
                if not res:
                    raise Exception("Average setting not specified for single metric %s (%s) in multi-metric chart %s (%s)" % (metric._data['name'], metric._id, self._data['name'], self._id))
                
                metric._data['moving_average_line_color'] = self._db.record[0]['moving_average_color']
                metric._data['moving_average_line_type'] = self._data['multi_metric_moving_average_line_type']
                metric._data['moving_average_line_width'] = self._data['multi_metric_moving_average_line_width']
                metric._data['metric_moving_average_label'] = '%s: Last %s Moving Average'%(metric._data['name'], metric._data['metric_moving_average_interval'])
                metric._show_moving_average = True
            else:
                metric._data['metric_moving_average_interval'] = ''
                metric._show_moving_average = False
            
            
            if int(row['axis_number']) == 1:
                metric._data['display_mask_id'] = self._data['multi_chart_primary_axis_display_mask_id']
            else:
                metric._data['display_mask_id'] = self._data['multi_chart_secondary_axis_display_mask_id']
                
            metric._data['metric_chart_display_type'] = row['metric_chart_display_type']
            metric._data['line_type'] = row['line_type']
            metric._data['axis_number'] = row['axis_number']
            metric._data['metric_color'] = row['metric_color']
            metric._data['metric_show_min_ever_on_chart_ind'] = 'N'
            metric._data['metric_show_max_ever_on_chart_ind'] = 'N'
            
            metric.init()
            
            metric._show_stop_light = False
            metric._show_std_deviation = False
            
            metric.formatter = self.formatter
            
            metrics[row['element_id']] = metric
        
        #if not (self.stoplight_element_id and metrics.has_key(self.stoplight_element_id)):
        if not (self.stoplight_element_id and self.stoplight_element_id in metrics):
            self.stoplight_element_id = 0
        
        return metrics

    def prepare_metric(self):
        """
        create metric dict main meta file
        """
        metrics = list()
        for metric_id in self._metrics:
            metric_value = dict()
            metric = self._metrics[metric_id]
            metric_value['metric_element_id'] = metric._data['element_id']
            metric_value['metric_name'] = metric._data['name']
            metric_value['metric_descr'] = metric._data['description']
            metric_value['metric_dashboard_category'] = metric._data['category']
            metric_value['metric_primary_topic'] = metric._data['topic_name']
            metric_value['metric_business_owner'] = metric._data['business_owner']
            metric_value['metric_tech_owner'] = metric._data['technical_owner']
            metric_value['metric_interval_id'] = metric._data['measurement_interval_id']
            metric_value['metric_moving_average_interval'] = metric._data['metric_moving_average_interval']
            

            metric_value['curr_value'] = metric.formatter.format_full(metric._get_curr_value(), metric._data['display_mask_id'])
            metric_value = metric.prepare_min_max(metric_value)
#            """
#            if metric.min_ever:
#                #metric_value['min_value'] = metric.formatter.format_half(metric.min_ever['value'], metric._data['display_mask_id'])
#                metric_value['min_value'] = metric.formatter.format_full(metric.min_ever['value'], metric._data['display_mask_id'])
#                metric_value['min_reached_on'] = metric.formatter.format_date(metric.min_ever['measurement_time'])
#            else:
#                metric_value['min_value'] = ''
#                metric_value['min_reached_on'] = ''
#            if metric.max_ever:
#                #metric_value['max_value'] = metric.formatter.format_half(metric.max_ever['value'], metric._data['display_mask_id'])
#                metric_value['max_value'] = metric.formatter.format_full(metric.max_ever['value'], metric._data['display_mask_id'])
#                metric_value['max_reached_on'] = metric.formatter.format_date(metric.max_ever['measurement_time'])
#            else:
#                metric_value['max_value'] = ''
#                metric_value['max_reached_on'] = ''
#            """
            metric_value['compare_to'] = ''

            metrics.append(metric_value)
        return metrics

    def get_y_axis_format(self):
        """
        get left y axis format
        """
        return self._get_y_axis_format(self._data['multi_chart_primary_axis_display_mask_id'])

    def get_y2_axis_format(self):
        """
        get left y axis format
        """
        if self._data['multi_chart_axis_count'] == 2:
            return self._get_y_axis_format(self._data['multi_chart_secondary_axis_display_mask_id'])
        return None

        
    def _get_x_title(self):
        """
        get x-axis title
        """
        return self._data['chart_x_axis_label']

    def _get_y_title(self, charting_interval):
        """
        get y-axis title
        """
        #if charting_interval and charting_interval.has_key('chart_title_display_suffix'):
        if charting_interval and 'chart_title_display_suffix' in charting_interval:
            #return self._data['multi_chart_primary_axis_unit_of_measure'] + ' - ' + charting_interval['chart_title_display_suffix']
            return "%s - %s" % (self._data['multi_chart_primary_axis_unit_of_measure'], charting_interval['chart_title_display_suffix'])
        return self._data['multi_chart_primary_axis_unit_of_measure']

    def _get_y2_title(self, charting_interval):
        """
        get y2-axis title
        """
        if self._data['multi_chart_axis_count'] == 2:
            #if charting_interval and charting_interval.has_key('chart_title_display_suffix'):
            if charting_interval and 'chart_title_display_suffix' in charting_interval:
                #return self._data['multi_chart_secondary_axis_unit_of_measure'] + ' - ' + charting_interval['chart_title_display_suffix']
                return "%s - %s" % (self._data['multi_chart_secondary_axis_unit_of_measure'], charting_interval['chart_title_display_suffix'])
            return self._data['multi_chart_secondary_axis_unit_of_measure']
        return ''    

    def get_min_max_ever_from_db(self):
        """
        get minimum/maximum ever dict
        """
        for metric_id in self._metrics:
            self._metrics[metric_id]._segment_value_id = self._segment_value_id
            self._metrics[metric_id].get_min_max_ever_from_db()



    def process_chart_interval(self, charting_interval):
        """
        draw charts for specified charting interval
        """
        self.formatter.set_def_date_format_id(charting_interval['display_mask_id'])
        
        for metric_id in self._metrics:
            self._metrics[metric_id].formatter.set_def_date_format_id(charting_interval['display_mask_id'])
        
        self.formatter.set_def_date_format_id(charting_interval['display_mask_id'])
        
        # get period for fetching measured values
        end_date, start_date, x_scale_values = self.get_interval(charting_interval)
        
        # init dictionary
        data = self.init_charting_data(list())
        if self.stoplight_element_id:
            data['thin_by_metric_id'] = self.stoplight_element_id
        else:
            for metric_id in self._metrics:
                data['thin_by_metric_id'] = metric_id
                break
      
        # all collected headers        
        orig_header = list()
        
        #process all metrics
        for metric_id in self._metrics:
            self._metrics[metric_id].fetch_interval_values(end_date, start_date)
            if self.stoplight_element_id and self.stoplight_element_id == metric_id:
                if self._expired_date and (not self._metrics[metric_id].measured_values or self._expired_date > self._metrics[metric_id].measured_values[-1]['measurement_time']):
                    
                    self._metrics[metric_id]._expired_date = self._expired_date
                    self._metrics[metric_id].spread_to_expired_date(start_date, x_scale_values)
                    self._expired_date = self._metrics[metric_id]._expired_date
                else:
                    self._expired_date = None
            #collect common headers for all metrics
            for date in self._metrics[metric_id].measured_values:
                if date['measurement_time'] not in orig_header:
                    orig_header.append(date['measurement_time'])

        
        
        orig_header.sort()
        
        data = self.create_headers(data, orig_header, x_scale_values)
        
        all_annotations = list()
        metric_order = 0
        for metric_id in self._metrics:
            metric_orig_headers = self._metrics[metric_id].fetch_orig_headers()
            # extend all metric headers by common header values if needed
            for date in orig_header:
                if date not in metric_orig_headers:
                    # we need to insert new date
                    k = 0
                    inserted = False
                    # look for position for new date element
                    for k, v in enumerate(metric_orig_headers):
                        if date < v:
                            inserted = True
                            break
                    
                    # insert new date to the last place
                    if metric_orig_headers:
                        if not inserted:
                            k += 1
                    # insert new date to the first place 
                    else:
                        k = 0
                       
                    fake_measured_value = dict()
                    fake_measured_value['measurement_time'] = date
                    fake_measured_value['moving_average_value'] = None
                    fake_measured_value['metric_measured_value_id'] = 0
                    fake_measured_value['metric_id'] = metric_id  
                    fake_measured_value['measurement_value'] = None
                    fake_measured_value['formatted_measurement_value'] = ''
                    #insert fake point
                    self._metrics[metric_id].measured_values.insert(k, fake_measured_value)
                    metric_orig_headers = self._metrics[metric_id].fetch_orig_headers()
            #data = self._metrics[metric_id].prepare_measured_values(data, end_date, start_date)
            data = self._metrics[metric_id].prepare_measured_values(data)
            
            # get annotations
            all_annotations.extend(self._metrics[metric_id].get_all_annotations(data['header'], data['orig_header'], data['rows'][metric_id]['data_settings']['data'], metric_order))
            metric_order += 1

        # index point/range annotations
        indexed_all_annotations = self.index_annotations(all_annotations)
        header_len = len(data['header'])

        # divide all annotations into point/range lists
        for metric_id in self._metrics:
            data['annotations'][metric_id], data['range_annotations'][metric_id] = self._metrics[metric_id].parse_annotations(indexed_all_annotations, header_len)

        data['primary_axis_display_mask'] = self.get_y_axis_format()
        data['secondary_y_axis_display_mask'] = self.get_y2_axis_format()
        
        # set titles
        data['x_axis_title'] = self._get_x_title()
        data['y_axis_title_left'] = self._get_y_title(charting_interval)
        data['y_axis_title_right'] = self._get_y2_title(charting_interval)
        
        data['preview_y_axis_title_left'] = self._data['multi_chart_primary_axis_unit_of_measure']
        data['preview_y_axis_title_right'] = self._data['multi_chart_secondary_axis_unit_of_measure']
        
        data['y_axis_num'] = self._data['multi_chart_axis_count']
        
        data['expired_date'] = self._expired_date
        
        orig_map = self.chart_gen.metric_chart(self._id, charting_interval['charting_interval_id'], data, self._jfile)
        
        map = list()
        annotations = list()
        
        # fetch image map data for json
        for metric_id in self._metrics:
            map.extend(self._metrics[metric_id]._prepare_map(orig_map, data))
            annotations.extend(self._metrics[metric_id].annotations)
            
            
        # write json
        self._jfile.make_chart_interval(map, charting_interval['charting_interval_id'], charting_interval, annotations)
        
        
        
        #create thumbnail/preview if it's not index interval only mode 
        if (not self.index_interval_only) and charting_interval['index_charting_interval_ind'] == 'Y':
            thumb_data = copy.deepcopy(data)
            # remove metrics not included into thumbnail
            for metric_id in self._metrics:
                if self._metrics[metric_id]._data['include_metric_in_thumbnail_ind'] != 'Y':
                    del thumb_data['rows'][metric_id]
            self.chart_gen.metric_thumbnail(self._id, charting_interval['charting_interval_id'],  thumb_data, self._jfile)
            self.chart_gen.metric_preview(self._id, charting_interval['charting_interval_id'],  data, self._jfile)
