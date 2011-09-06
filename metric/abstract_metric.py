# -*- coding: utf-8 -*-

from file_man.jfiles import JMetricFile
from conf import ConfigReader
from db.db_conn import DBManager
import datetime
from formatter import FieldFormatter
from time import mktime
from operator import itemgetter
from file_man.flocker import FileLock
import os
from pprint import pprint

class AbstractMetricElement:
    """
    Abstract metric dashboard element
    """
    _path = ''
    _type = ''
    _charting_intervals = []
    _id = 0
    _jfile = None
    _segment_value_id = 0
    _segment_value = 0
    formatted_headers = []
    orig_headers = []
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

    _date_format_rule = None
    _preview_date_format_rule = None

    def __init__(self, id, index_interval_only):
        self.config = ConfigReader()
        self._db = DBManager.get_query()

        self._id = id
        self.index_interval_only = index_interval_only
        # fetch all element data
        self._data = self._get_element()

    def init(self):
        self.formatter = FieldFormatter(self._data['def_date_mask_id'])
        self.preview_formatter = FieldFormatter(self._data['def_date_mask_id'])

        # fetch segments
        self._segment = self._get_segment()
        self._segment_values = self._get_segment_values()

        # get all charting intervals
        self._charting_intervals = self._get_charting_intervals()
        self._date_format_rule, self._preview_date_format_rule = self._get_date_format_rule()

        self.formatter.set_custom_date_format_rule(self._date_format_rule)
        self.preview_formatter.set_custom_date_format_rule(self._preview_date_format_rule)

        if self._data['metric_stoplight_calc_method'] and self._data['metric_stoplight_calc_method'] != 'None':
            self._show_stop_light = True

        self._jfile = JMetricFile(self._path, self._id)

        self._jfile.set_segment(self._segment)

    def _get_segment(self):
        """
        Get segment
        """
        segment = []
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
        segment_values = []
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
        preview_date_format_rule = ''
        if self._data['measurement_interval_id']:
            res = self._db.Query("""SELECT date_format_string, preview_display_format_string
                    FROM measurement_interval
                WHERE
                    `measurement_interval_id`=%s""",(self._data['measurement_interval_id']))
            if res:
                data = self._db.record[0]
                date_format_rule = data['date_format_string']
                preview_date_format_rule = data['preview_display_format_string']
        if not date_format_rule:
            date_format_rule = '%Y-%m-%d %T'
        if not preview_date_format_rule:
            preview_date_format_rule = '%Y-%m-%d %T'
        return date_format_rule, preview_date_format_rule

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

            charting_intervals = [interval for interval in self._db.record]
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

#    def get_fiscal_interval_xtd_start_date(self, charting_interval, end_date):
#        """
#        get start date
#        """
#
#        curr_date = end_date
#
#        if not isinstance(curr_date, datetime.datetime):
#            curr_date = datetime.datetime.combine(curr_date, datetime.time.min)
#        else:
#            curr_date = curr_date.replace(hour=0, minute=0, second=0, microsecond=0)
#        fiscal_date = self._get_fiscal_date(curr_date, charting_interval['interval_unit'])
#
#        if fiscal_date:
#            start_date = fiscal_date['first_day_of_period']
#        else:
#            # this situation is not permitted
#            start_date = self.get_interval_xtd_start_date(charting_interval, end_date)
#
#        if isinstance(start_date, datetime.datetime):
#            start_date = start_date.replace(hour=0, minute=0, second=0, microsecond=0)
#        else:
#            start_date = datetime.datetime.combine(start_date, datetime.time.min)
#
#        return start_date

#    def get_interval_xtd_start_date(self, charting_interval, end_date):
#        """
#        get start date
#        """
#        start_date = end_date
#
#        if not isinstance(start_date, datetime.datetime):
#            start_date = datetime.datetime.combine(start_date, datetime.time.min)
#        else:
#            start_date = start_date.replace(hour=0, minute=0, second=0, microsecond=0)
#
#        if charting_interval['interval_unit'] == 'year':
#            start_date = start_date.replace(month=1, day=1)
#        elif charting_interval['interval_unit'] == 'month':
#            res = self._db.Query("""SELECT first_day_of_month
#                                        FROM calendar_month
#                                    WHERE
#                                        `first_day_of_month` <= %s
#                                    ORDER BY `month_id` DESC
#                                    LIMIT 0, 1""", (end_date,))
#            if res:
#                date = self._db.record[0]
#                start_date = date['first_day_of_month']
#            else:
#                start_date.replace(day=1)
#
#        elif charting_interval['interval_unit'] == 'quarter':
#            res = self._db.Query("""SELECT first_day_of_quarter
#                                        FROM calendar_quarter
#                                    WHERE
#                                        `first_day_of_quarter` <= %s
#                                    ORDER BY `quarter_id` DESC
#                                    LIMIT 0, 1""", (end_date, ))
#            if res:
#                date = self._db.record[0]
#                start_date = date['first_day_of_quarter']
#            else:
#                # get current quarter 0-3
#                current_quarter = (start_date.month-1) // 3
#                current_first_month_of_quarter = current_quarter * 3 + 1
#                start_date = start_date.replace(day=1, month=current_first_month_of_quarter)
#
#        elif charting_interval['interval_unit'] == 'week':
#            #if self._data['week_display_prefix'] == 'week starting':
#            if charting_interval['weekly_data_shown_for'] == 'start of week':
#                compare_week_day = 'first_day_of_week'
#            else:
#                compare_week_day = 'last_day_of_week'
#            res = self._db.Query("""SELECT `first_day_of_week`,
#                                           `last_day_of_week`
#                                        FROM calendar_week
#                                    WHERE
#                                        `%s` <= %%s
#                                    ORDER BY `week_id` DESC
#                                    LIMIT 0, 1""" % compare_week_day, (end_date, ))
#
#            if res:
#                date = self._db.record[0]
#                #if self._data['week_display_prefix'] == 'week starting':
#                if charting_interval['weekly_data_shown_for'] == 'start of week':
#                    start_date = date['first_day_of_week']
#                else:
#                    start_date = date['last_day_of_week']
#            else:
#                prev_week = end_date - datetime.timedelta(days = 7)
#                prev_week_day_of_week = prev_week.weekday()
#                #if self._data['week_display_prefix'] == 'week starting':
#                if charting_interval['weekly_data_shown_for'] == 'start of week':
#                    start_date = prev_week - datetime.timedelta(days=prev_week_day_of_week)
#                else:
#                    start_date = prev_week + datetime.timedelta(days=(6-prev_week_day_of_week))
#
#        if isinstance(start_date, datetime.datetime):
#            start_date = start_date.replace(hour=0, minute=0, second=0, microsecond=0)
#        else:
#            start_date = datetime.datetime.combine(start_date, datetime.time.min)
#
#        return start_date

    def get_fiscal_interval_start_date(self, charting_interval, end_date):
        """
        get start date for selected charting interval
        """
        start_date = None
        charting_interval_unit = charting_interval['charting_interval_unit']
        charting_interval_value = charting_interval['charting_interval_value']
        interval_unit = charting_interval['fiscal_period_type']

        current_date = self._get_fiscal_date(end_date, interval_unit)

        # get date of charting_interval_unit * charting_interval_value time ago
        res = self._db.Query("""SELECT *
                        FROM fiscal_%s
                        WHERE fiscal_%s_id <= %%s
                        ORDER BY fiscal_%s_id DESC
                        LIMIT %%s, 1""" %
                                     (charting_interval_unit, # FROM fiscal_%s
                                      charting_interval_unit,# WHERE fiscal_%s_id
                                      charting_interval_unit), # ORDER BY fiscal_%s_id DESC
                (current_date['fiscal_%s_id' % charting_interval_unit], #  <= %%s
                 charting_interval_value) #LIMIT %%s, 1
        )

        if res:
            charting_interval_period_start = self._db.record[0]
            if interval_unit == charting_interval_unit:
                start_date = charting_interval_period_start['first_day_of_%s' % self._fiscal_field_name(charting_interval_unit)]
            else:
                start_charting_interval_period_id = charting_interval_period_start['fiscal_%s_id' % charting_interval_unit]

                x_number_name = '%s_number' % self._fiscal_field_name(interval_unit)
                current_date_number = current_date[x_number_name]

                res = self._db.Query("""SELECT *
                        FROM fiscal_%s
                        WHERE %s = %%s
                            AND fiscal_%s_id = %%s
                        LIMIT 0, 1""" %
                                     (interval_unit, # FROM fiscal_%s
                                      x_number_name, # WHERE %s =
                                      charting_interval_unit
                                      ),
                        (current_date_number,
                        start_charting_interval_period_id)
                )
                if res:
                    charting_interval_period_start_date = self._db.record[0]
                    start_date = charting_interval_period_start_date['first_day_of_%s' % self._fiscal_field_name(interval_unit)]

        if start_date is None:
            start_date = self.get_interval_start_date(charting_interval, end_date)

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

    def get_interval_start_date(self, charting_interval, end_date):
        """
        get start date for selected charting interval
        """
        charting_interval_unit = charting_interval['charting_interval_unit']
        charting_interval_value = charting_interval['charting_interval_value']

        if charting_interval_unit == 'week':
            charting_interval_unit = 'day'
            charting_interval_value = charting_interval['charting_interval_value'] * 7

        self._db.Query("""SELECT DATE_SUB(%%s, INTERVAL %%s %s) AS start_date""" % charting_interval_unit,
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

    def get_fiscal_interval_end_date(self, charting_interval, last_date):
        """
        get end date for selected charting interval
        """
        real_last_date = last_date

        if not last_date:
            #last_date = datetime.datetime.today()
            last_date = datetime.datetime.now()
        #end_date = last_date

        fiscal_period_type = charting_interval['fiscal_period_type']
        if False: #if self._data['omit_partial_periods_ind'] == 'N' and not real_last_date:
            end_date = last_date
        else:
            fiscal_date = self._get_fiscal_date(last_date, charting_interval['fiscal_period_type'])

            if fiscal_date:
                if not real_last_date:
                    end_date = fiscal_date['first_day_of_period']
                else:
                    res = self._db.Query("""SELECT * FROM fiscal_%s WHERE fiscal_%s_id < %%s ORDER BY fiscal_%s_id DESC LIMIT 0, 1""" %
                                                 (fiscal_period_type, fiscal_period_type, fiscal_period_type), (fiscal_date['fiscal_%s_id' % fiscal_period_type]))
                    if res:
                        fend_date = self._db.record[0]
                        end_date = fend_date['first_day_of_period']
                    else:
                        end_date = self.get_interval_end_date(charting_interval, real_last_date)
            else:
                end_date = self.get_interval_end_date(charting_interval, real_last_date)
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

    def get_interval_end_date(self, charting_interval, last_date):
        """
        get end date for selected charting interval
        """
        #real_last_date = last_date

        if not last_date:
            #last_date = datetime.datetime.today()
            last_date = datetime.datetime.now()

        #end_date = datetime.datetime.today()
        #end_date = datetime.datetime.now()
        end_date = last_date

        # minute
        if charting_interval['interval_unit'] == 'minute':
            end_date = last_date - datetime.timedelta(minutes=1)
            if isinstance(end_date, datetime.datetime):
                end_date = end_date.replace(second=0, microsecond=0)

        # hour
        elif charting_interval['interval_unit'] == 'hour':
            end_date = last_date - datetime.timedelta(hours=1)
            if isinstance(end_date, datetime.datetime):
                end_date = end_date.replace(minute=0, second=0, microsecond=0)

        # day
        elif charting_interval['interval_unit'] == 'day':
            if False: #if self._data['omit_partial_periods_ind'] == 'N' and not real_last_date:
                end_date = last_date
            else:
                end_date = last_date - datetime.timedelta(days=1)
                if isinstance(end_date, datetime.datetime):
                    end_date = end_date.replace(hour=0, minute=0, second=0, microsecond=0)

        # month
        elif charting_interval['interval_unit'] == 'month':
            if False:#if self._data['omit_partial_periods_ind'] == 'N' and not real_last_date:
                end_date = last_date
            else:
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
                    end_date = end_date.replace(hour=0, minute=0, second=0, microsecond=0)

        # week
        elif charting_interval['interval_unit'] == 'week':
            if False: #if self._data['omit_partial_periods_ind'] == 'N' and not real_last_date:
                end_date = last_date
            else:
                #if self._data['week_display_prefix'] == 'week starting':
                if charting_interval['weekly_data_shown_for'] == 'start of week':
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
                    #if self._data['week_display_prefix'] == 'week starting':
                    if charting_interval['weekly_data_shown_for'] == 'start of week':
                        end_date = date['first_day_of_week']
                    else:
                        end_date = date['last_day_of_week']
                else:
                    prev_week = last_date - datetime.timedelta(days = 7)
                    prev_week_day_of_week = prev_week.weekday()
                    #if self._data['week_display_prefix'] == 'week starting':
                    if charting_interval['weekly_data_shown_for'] == 'start of week':
                        end_date = prev_week - datetime.timedelta(days = prev_week_day_of_week)
                    else:
                        end_date = prev_week + datetime.timedelta(days = (6 - prev_week_day_of_week))

                if isinstance(end_date, datetime.datetime):
                    end_date = end_date.replace(hour=0, minute=0, second=0, microsecond=0)

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

                current_first_month_of_quarter = current_quarter * 3 + 1
                current_quarter_first_day = today.replace(day=1, month=current_first_month_of_quarter)

                year, month, day = current_quarter_first_day.timetuple()[:3]
                new_month = month - 6

                end_date = datetime.datetime(year + (new_month / 12), new_month % 12, day)
            if isinstance(end_date, datetime.datetime):
                end_date = end_date.replace(hour=0, minute=0, second=0, microsecond=0)
        # year
        elif charting_interval['interval_unit'] == 'year':
            today = last_date
            year_beginning = today.replace(day=1, month=1, hour=0, minute=0, second=0)
            if last_date > year_beginning:
                end_date = year_beginning
            else:
                end_date = year_beginning.replace(year=year_beginning.year - 1)

            if isinstance(end_date, datetime.datetime):
                end_date = end_date.replace(microsecond=0)

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

    def get_last_meas_date(self):
        pass

    def get_interval_end_start_dates(self, charting_interval):
        # default from date
#        if charting_interval['xtd_interval_ind'] == 'Y':
#            if charting_interval['measurement_interval_button_name'] ==  'MTD':
#                charting_interval['origin_interval_unit'] = 'month'
#            elif charting_interval['measurement_interval_button_name'] ==  'QTD':
#                charting_interval['origin_interval_unit'] = 'quarter'
#            elif charting_interval['measurement_interval_button_name'] ==  'YTD':
#                charting_interval['origin_interval_unit'] = 'year'
#            charting_interval['interval_unit'] = 'day'

        # last date of charting interval period
        #end_date_val = self.get_interval_end_date(charting_interval, default_end_date)
        end_date_val = self.get_last_meas_date()

        if charting_interval['fiscal_period_type']:
            real_end_date_val = self.get_fiscal_interval_end_date(charting_interval, None)
        else:
            real_end_date_val = self.get_interval_end_date(charting_interval, None)

        if not end_date_val:
            end_date_val = real_end_date_val
        elif self._data['omit_partial_periods_ind'] != 'N' and end_date_val > real_end_date_val and charting_interval['interval_unit'] in ('day', 'month', 'year'):
            end_date_val = real_end_date_val

        if charting_interval['fiscal_period_type']:
            start_date_val = self.get_fiscal_interval_start_date(charting_interval, end_date_val)
#            if charting_interval['xtd_interval_ind'] == 'Y':
#                start_date_val = self.get_fiscal_interval_xtd_start_date(charting_interval, end_date_val)
#            else:
#                start_date_val = self.get_fiscal_interval_start_date(charting_interval, end_date_val)
        else:
            start_date_val = self.get_interval_start_date(charting_interval, end_date_val)
#            if charting_interval['xtd_interval_ind'] == 'Y':
#                start_date_val = self.get_interval_xtd_start_date(charting_interval, end_date_val)
#            else:
#                # first date of charting interval period
#                start_date_val = self.get_interval_start_date(charting_interval, end_date_val)
        return end_date_val, start_date_val

    def get_interval(self, charting_interval):
        """
        get time interval (start and end date) for selected charting interval
        """
        end_date_val, start_date_val = self.get_interval_end_start_dates(charting_interval)
        
        self._expired_date = None

        if self._data['show_expired_zone_ind'] and self._data['max_time_before_expired_sec']:
            last_valid_date = datetime.datetime.today() - datetime.timedelta(seconds=self._data['max_time_before_expired_sec'])
            # if expired date is in interval. if not then do not draw expired zone
            if last_valid_date > end_date_val:
                self._expired_date = end_date_val
                end_date_val = datetime.datetime.now()
                while end_date_val > last_valid_date:
                    if charting_interval['fiscal_period_type']:
                        end_date_val = self.get_fiscal_interval_end_date(charting_interval, end_date_val)
                    else:
                        end_date_val = self.get_interval_end_date(charting_interval, end_date_val)

        # fetch all dates where measured values supposed to be computed
        dates = [end_date_val]
        next_end_date_val = end_date_val

        while next_end_date_val > start_date_val:
            if charting_interval['fiscal_period_type']:
                next_end_date_val = self.get_fiscal_interval_end_date(charting_interval, next_end_date_val)
            else:
                next_end_date_val = self.get_interval_end_date(charting_interval, next_end_date_val)
            if next_end_date_val < start_date_val:
                break
            dates.append(next_end_date_val)
        dates.reverse()

        if charting_interval['xtd_interval_ind'] == 'Y':
            if charting_interval['fiscal_period_type']:
                xtd_interval = self._get_xtd_interval_fiscal(charting_interval, end_date_val, start_date_val)
            else:
                xtd_interval = self._get_xtd_interval(charting_interval, end_date_val, start_date_val)
        else:
            xtd_interval = []

        return end_date_val, start_date_val, dates, xtd_interval


    def _get_xtd_interval_fiscal(self, charting_interval, end_date, start_date):
        xtd_interval = []

        # month
        if charting_interval['xtd_reset_interval_unit'] == 'month':
            res = self._db.Query("""SELECT first_day_of_period AS `first_date`
                                        FROM fiscal_month
                                    WHERE
                                        `first_day_of_period` < %s
                                        AND `first_day_of_period` >= %s
                                    ORDER BY `fiscal_month_id` ASC""", (end_date, start_date))
        # quarter
        elif charting_interval['xtd_reset_interval_unit'] == 'quarter':
            res = self._db.Query("""SELECT first_day_of_quarter AS first_date
                                        FROM fiscal_quarter
                                    WHERE
                                        `first_day_of_quarter` < %s
                                        AND `first_day_of_quarter` >= %s
                                    ORDER BY `fiscal_quarter_id` ASC""", (end_date, start_date))
        # year
        elif charting_interval['xtd_reset_interval_unit'] == 'year':
            res = self._db.Query("""SELECT first_day_of_fiscal_year AS first_date
                                        FROM fiscal_year
                                    WHERE
                                        `first_day_of_fiscal_year` < %s
                                        AND `first_day_of_fiscal_year` >= %s
                                    ORDER BY `fiscal_year_id` ASC""", (end_date, start_date))
        else:
            res = 0

        if res:
            xtd_interval = [date['first_date'] for date in self._db.record]

        return self._normalize_date_list(xtd_interval)
        
    def _get_xtd_interval(self, charting_interval, end_date, start_date):
        xtd_interval = []

        # month
        if charting_interval['xtd_reset_interval_unit'] == 'month':
            res = self._db.Query("""SELECT first_day_of_month
                                        FROM calendar_month
                                    WHERE
                                        `first_day_of_month` < %s
                                        AND `first_day_of_month` >= %s
                                    ORDER BY `month_id` ASC""", (end_date, start_date))
            if res:
                xtd_interval = [date['first_day_of_month'] for date in self._db.record]
        # quarter
        elif charting_interval['xtd_reset_interval_unit'] == 'quarter':
            res = self._db.Query("""SELECT first_day_of_quarter
                                        FROM calendar_quarter
                                    WHERE
                                        `first_day_of_quarter` < %s
                                        AND `first_day_of_quarter` >= %s
                                    ORDER BY `quarter_id` ASC""", (end_date, start_date))
            if res:
                xtd_interval = [date['first_day_of_quarter'] for date in self._db.record]
        # year
        elif charting_interval['xtd_reset_interval_unit'] == 'year':
            today = end_date
            year_beginning = today.replace(day=1, month=1, hour=0, minute=0, second=0)

            xtd_date = year_beginning.replace(day=1)
            while start_date <= xtd_date < xtd_date:
                xtd_interval.append(xtd_date)
                xtd_date = xtd_date.replace(year=xtd_date.year - 1)

        return self._normalize_date_list(xtd_interval)

    def _normalize_date_list(self, dates):
        normalized_dates = []
        for date in dates:
            # convert to datetime type
            if isinstance(date, str):
                try:
                    date_val = datetime.datetime.strptime(date, '%Y-%m-%d %H:%M:%S')
                except ValueError:
                    try:
                        date_val = datetime.datetime.strptime(date, '%Y-%m-%d')
                    except ValueError:
                        raise Exception("Cannot format date '%s' to datetime for metric %s (%s)" % (date, self._data['name'], self._id))
            elif isinstance(date, datetime.datetime):
                date_val = date.replace(hour=0, minute=0, second=0, microsecond=0)
            elif isinstance(date, datetime.date):
                date_val = datetime.datetime.combine(date, datetime.time.min)
            else:
                # it shouldn't be so
                raise Exception("Cannot format date '%s' to datetime for metric %s (%s)" % (xtd_date, self._data['name'], self._id))
            normalized_dates.append(date_val)

        return normalized_dates

    def _get_y_axis_format(self, mask_id):
        """
        get y axis format
        """
        format = {}
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

    def prepare_metric(self, metrics):
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
        available_intervals = []

        metrics = self.prepare_metric(metrics=[])
        metrics.sort(key=itemgetter('metric_element_id'))

        # see related
        self._db.Query("""SELECT e.*
                                    FROM dashboard_element_topic det, dashboard_element e
                                WHERE e.element_id = det.dashboard_element_id
                                    AND dashboard_element_id <> %s
                                    AND e.enabled_ind = 'Y'
                                    AND topic_id IN (select topic_id from dashboard_element_topic where dashboard_element_id = %s)
                                    AND IFNULL(e.segment_id,0) = %s
                                    AND false
                            """,
                        (self._id, self._id, self._data['segment_id']))
        related = [related_element for related_element in self._db.record]

        # available measurement intervals
        self._db.Query("""
                        SELECT measurement_interval.*,
                                 dashboard_element.element_id
                            FROM dashboard_element
                            LEFT JOIN measurement_interval
                                ON measurement_interval.measurement_interval_id = dashboard_element.measurement_interval_id
                        WHERE
                            dashboard_element.shared_measure_id = %s
                            AND dashboard_element.`type` = 'metric'
                            AND IFNULL(dashboard_element.segment_id,0) = %s
                        GROUP BY measurement_interval.measurement_interval_id
                        ORDER BY
                                measurement_interval.display_sequence,
                                dashboard_element.name ASC
                        """, (self._data['shared_measure_id'], self._data['segment_id']))
        for interval in self._db.record:
            interval['report_data_set_instance_id'] = 0
            available_intervals.append(interval)

        # drill to
        self._db.Query("""
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
        drill_to = [drill_report for drill_report in self._db.record]

        available_views = ['standard']
        
        if self._show_stop_light:
            available_views.append('stoplight')

        if self._show_std_deviation:
            available_views.append('std_deviation')

        # last_updated_time
        res = self._db.Query("""SELECT * FROM last_dashboard_element_segment_value WHERE element_id = %s AND segment_value_id = %s""",
                        (self._id, self._data['segment_id']))
        if res:
            self._data['last_updated_time'] = self._db.record[0]['last_updated_time']
        else:
            self._data['last_updated_time'] = ''
        available_segments = self._get_non_empty_segments()

        available_charting_intervals = self._get_non_empty_charting_intervals()

        self._jfile.make_meta(self._data, available_charting_intervals, available_intervals, drill_to, related, metrics, available_segments, available_views)

    def _get_non_empty_charting_intervals(self):
        #self._charting_intervals
        pass

    def _get_non_empty_segments(self):
        pass

    def fetch_compare_lines(self, charting_interval):
        """
        get all compare lines info
        """
        compare_lines = []
        if self._data['metric_compare_line'] == 'standard':
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
        elif self._data['metric_compare_line'] == 'override' and self._data['metric_override_compare_line_id']:
            res = self._db.Query("""SELECT
                                    compare_line.*
                                FROM compare_line
                                WHERE
                                    compare_line_id = %s
                                    """, (self._data['metric_override_compare_line_id']))
            if res:
                compare_lines.append(self._db.record[0])

        return compare_lines

    def init_charting_data(self, compare_lines, xtd_interval):
        """
        Create empty dictionary with metric info for charting
        """
        data = {
            'header': [], # formatted header
            'fiscal_header': [], # fiscal header
            'orig_header': [], # original unformatted header
            'even_header': [], # proportional header. needed for correct x-axis charting thinned down values
            'x_axis': [], # lists of delta between first date and other dates of headers. needed for calculation simplified line for thinning down
            'rows': {}, # rows with data
            'point_annotations': {}, # list of annotations markers(True/False) - if any annotation exists for value
            'range_annotations': {},
            'thin_by_metric_uid': 0,
            'compare_lines': [compare_line['compare_line_id'] for compare_line in compare_lines], # set compare lines
            'x_scale_values': [], # labels for x-axis
            'x_scale_labels': [],
            'show_stop_light': self._show_stop_light,
            'show_std_deviation': self._show_std_deviation,
            'metric_change_bar_color_on_value_ind': self._data['metric_change_bar_color_on_value_ind'],
            'xtd_interval': xtd_interval
        }

        return data

    def create_headers(self, data, orig_headers, x_scale_values, fiscal_period_type):
        """
        create dict with original (datetime) header values and float values of header
        """
        for measurement_time in orig_headers:
            # set original header dates value
            data['orig_header'].append(measurement_time)
            # set formatted header dates values
            if fiscal_period_type:
                fiscal_date = self._get_fiscal_date(measurement_time, fiscal_period_type)
                if fiscal_date:
                    formatted_date = fiscal_date['name']
                    data['fiscal_header'].append(fiscal_date)
                else:
                    formatted_date = self.formatter.format_date(measurement_time)
                    data['fiscal_header'].append(None)
            else:
                formatted_date = self.formatter.format_date(measurement_time)
            data['header'].append(formatted_date)
        if orig_headers:
            f_date = orig_headers[0]
            data['x_axis'] = [float(repr(mktime(orig_header.timetuple()) - mktime(f_date.timetuple()))) for orig_header in data['orig_header']]
            data['even_header'] = range(len(data['header']))

            first_date = orig_headers[0]
            last_date = orig_headers[-1]
            data['x_scale_values'] = filter(lambda x_scale_value: x_scale_value <= last_date and x_scale_value >= first_date, x_scale_values)
            for x_scale_value in data['x_scale_values']:
                if fiscal_period_type:
                    fiscal_date = self._get_fiscal_date(x_scale_value, fiscal_period_type)
                    if fiscal_date:
                        formatted_date = fiscal_date['name']
                    else:
                        formatted_date = self.formatter.format_date(x_scale_value)
                else:
                    formatted_date = self.formatter.format_date(x_scale_value)

                data['x_scale_labels'].append(formatted_date)
        
        return data

    def _fiscal_field_name(self, fiscal_period_type):
        """
        Fix for mixed fields names of fiscal_* tables
        """
        if fiscal_period_type == 'year':
            return 'fiscal_year'
        elif fiscal_period_type == 'quarter':
            return 'quarter'
        elif fiscal_period_type == 'month':
            return 'period'


    def _get_fiscal_date(self, date_time, fiscal_period_type):
        """
        Returns fiscal date from fiscal_* tables
        """
        fiscal_field_name = self._fiscal_field_name(fiscal_period_type)
        sql_query = """SELECT *,
                        first_day_of_%s AS first_day_of_period,
                        last_day_of_%s AS last_day_of_period
                    FROM fiscal_%s
                    WHERE %%s >= first_day_of_%s AND %%s <= last_day_of_%s""" % (
                        fiscal_field_name,
                        fiscal_field_name,
                        fiscal_period_type,
                        fiscal_field_name,
                        fiscal_field_name)

        res = self._db.Query(sql_query, (date_time, date_time))
        if res:
            return self._db.record[0]
        return None

    def unlock(self):
        if self._filelocker:
            self._filelocker.release()
        self._filelocker = None

    def set_logger(self, logger):
        self._logger = logger

    def process_chart_interval(self, charting_interval):
        pass

    def _set_current_segment(self, segment_value):
        """
        set current segment value
        """
        pass

    def update(self, segment_value_id, charting_interval_id):
        """
        main class for generation metric
        """

        #self.chart_gen = ChartGenerator(self.formatter)

        #chart only index interval
        if self.index_interval_only:
            charting_interval_id = self.index_interval_id

        #list of segments or zero for non-segmented
        segments = []
        if segment_value_id and self._segment and any(segment['segment_value_id'] == segment_value_id for segment in self._segment_values):
            segments = list(segment for segment in self._segment_values if segment['segment_value_id']==segment_value_id)
        elif self._segment_values:
            segments = self._segment_values
        else:
            segments.append(0)

        for segment_value in segments:
            self._set_current_segment(segment_value)

            self._jfile.set_segment_value(segment_value)
            self._jfile.set_data(self._data)

            #self._filelocker = FileLock("%s%s/run_segment_%s" % (self._path, self._id, self._segment_value_id), 0, 0)
            self._filelocker = FileLock(os.path.join(self._path, str(self._id), "run_segment_%s" % self._segment_value_id), 0, 0)

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
