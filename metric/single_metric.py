# -*- coding: utf-8 -*-

from abstract_metric import AbstractMetricElement
from simplejson.ordered_dict import OrderedDict
import copy
from chart.font_manager import FontManager
from operator import itemgetter
from chart.metric_chart import MetricChart
from pprint import pprint

class MetricElement(AbstractMetricElement):
    """
    Single metric dashboard element
    """
    min_max_ever =  {}

    # needed for storing settings for multimetric charting. used only by parent multimetric
    sub_settings = {}

    measured_values = []
    annotations = []
    range_annotations = []
    all_annotations = []
    uid = 0
    is_name_with_segment = False
    _average_with_name = False

    def __init__(self, id, index_interval_only):
        AbstractMetricElement.__init__(self, id, index_interval_only)
        self._type = 'metric'
        self._path = self.config.metric_root
        self.uid = 0
        self.is_name_with_segment = False
        self._average_with_name = False


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
            self._show_moving_average = True

        if self._show_moving_average or (data['alert_prior_measurement_value_count'] and data['interval_unit']):
            data['metric_std_deviation_moving_average_label'] = 'Last %s %s Moving Average' % (data['alert_prior_measurement_value_count'], data['interval_unit'])
            data['metric_std_deviation_moving_average_interval'] = '%s %s' % (data['alert_prior_measurement_value_count'], data['interval_unit'])
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

    def _get_metric_label(self):
        """
        get metric label - name or name with segment value
        """
        name = self._data['name']
        if self._data['segment_id'] and self.is_name_with_segment:
            name_parts = []
            if self._segment['segment_value_placement'] == 'before element name':
                name_parts.append(self._segment['segment_value_prefix'])
                name_parts.append(self._segment_value['value_display_name'])
                name_parts.append(self._segment['segment_value_suffix'])
                name_parts.append(name)
            elif self._segment['segment_value_placement'] == 'after element name':
                name_parts.append(name)
                name_parts.append(self._segment['segment_value_prefix'])
                name_parts.append(self._segment_value['value_display_name'])
                name_parts.append(self._segment['segment_value_suffix'])
            name_parts = [unicode(name_part).strip() for name_part in name_parts if name_part]
            name = ' '.join(name_parts)
        return name

    def _get_metric_moving_average_label(self):
        """
        get metric moving average label
        """
        if self._average_with_name:
            return '%s: Last %s Moving Average' % (self._get_metric_label(), self._data['metric_moving_average_interval'])
        else:
            return 'Last %s Moving Average' % self._data['metric_moving_average_interval']

    def prepare_measured_values(self, data, shape_id=None):
        """
        create dict with data for passing to chart generator
        """
        data[self.uid] = {}

        # get min/max values for interval
        data[self.uid]['min_for_interval'] = self.get_min_for_interval()
        data[self.uid]['max_for_interval'] = self.get_max_for_interval()

        data[self.uid]['expired_date'] = self._expired_date

        data['rows'][self.uid] = OrderedDict()

        data['rows'][self.uid]['data_settings'] = {}
        data['rows'][self.uid]['data_settings']['label'] = self._get_metric_label()
        data['rows'][self.uid]['data_settings']['data'] = []
        data['rows'][self.uid]['data_settings']['axis_number'] = self._data['axis_number']
        data['rows'][self.uid]['data_settings']['display_type'] = self._data['metric_chart_display_type']
        data['rows'][self.uid]['data_settings']['line_type'] = self._data['line_type']
        data['rows'][self.uid]['data_settings']['line_style'] = self._data['metric_chart_line_style']
        data['rows'][self.uid]['data_settings']['color'] = FontManager.get_db_color(self._data['metric_color'])
        data['rows'][self.uid]['data_settings']['line_width'] = self._data['line_width']
        data['rows'][self.uid]['data_settings']['shape_id'] = shape_id

        data['rows'][self.uid]['min_ever_settings'] = None
        data['rows'][self.uid]['max_ever_settings'] = None
        data['rows'][self.uid]['average_settings'] = None
        data['rows'][self.uid]['stop_light'] = None
        data['rows'][self.uid]['compare_settings'] = None
        avg_interval = ''

        #if self._data['metric_moving_average_interval']:
        if self._show_moving_average or self._show_std_deviation:
            data['rows'][self.uid]['average_settings'] = {}

            data['rows'][self.uid]['average_settings']['show_moving_average'] = self._show_moving_average
            data['rows'][self.uid]['average_settings']['show_std_deviation'] = self._show_std_deviation

            if self._show_moving_average:
                data['rows'][self.uid]['average_settings']['label'] = self._get_metric_moving_average_label()
                avg_interval = self._data['metric_moving_average_interval']
            elif self._show_std_deviation:
                data['rows'][self.uid]['average_settings']['label'] = self._data['metric_std_deviation_moving_average_label']
                avg_interval = self._data['metric_std_deviation_moving_average_interval']

            data['rows'][self.uid]['average_settings']['std_deviation_label'] = ''

            data['rows'][self.uid]['average_settings']['metric_unusual_value_std_dev'] = 0

            if self._show_std_deviation:
                data['rows'][self.uid]['average_settings']['metric_unusual_value_std_dev'] = self._data['metric_unusual_value_std_dev']
                if self._data['metric_unusual_value_std_dev'] == 1:
                    data['rows'][self.uid]['average_settings']['std_deviation_label'] = '1 standard deviation'
                else:
                    data['rows'][self.uid]['average_settings']['std_deviation_label'] = '%s standard deviations' % self._data['metric_unusual_value_std_dev']

            data['rows'][self.uid]['average_settings']['data'] = []
            data['rows'][self.uid]['average_settings']['std_deviation_data'] = []
            data['rows'][self.uid]['average_settings']['axis_number'] = self._data['axis_number']
            data['rows'][self.uid]['average_settings']['display_type'] = 'line'
            data['rows'][self.uid]['average_settings']['color'] = FontManager.get_db_color(self._data['moving_average_line_color'])
            data['rows'][self.uid]['average_settings']['line_type'] = self._data['moving_average_line_type']
            data['rows'][self.uid]['average_settings']['line_width'] = self._data['moving_average_line_width']
            data['rows'][self.uid]['average_settings']['line_style'] = self._data['moving_average_line_style']

        if self._show_stop_light:
            data['rows'][self.uid]['stop_light'] = {}
            data['rows'][self.uid]['stop_light']['display_type'] = 'area'
            data['rows'][self.uid]['stop_light']['color'] = 0
            data['rows'][self.uid]['stop_light']['metric_more_is_better_ind'] = self._data['metric_more_is_better_ind']
            data['rows'][self.uid]['stop_light']['good'] = []
            data['rows'][self.uid]['stop_light']['bad'] = []
            data['rows'][self.uid]['stop_light']['deviation'] = []

        for measured_value in self.measured_values:
            # pre format measured value
            value = self.formatter.pre_format(measured_value['measurement_value'], self._data['display_mask_id'])
            data['rows'][self.uid]['data_settings']['data'].append(value)

            # stop light values
            if self._show_stop_light:
                bad_value = self.formatter.pre_format(measured_value['stoplight_bad_threshold_value'], self._data['display_mask_id'])
                data['rows'][self.uid]['stop_light']['bad'].append(bad_value)
                good_value = self.formatter.pre_format(measured_value['stoplight_good_threshold_value'], self._data['display_mask_id'])
                data['rows'][self.uid]['stop_light']['good'].append(good_value)
                deviation_value = self.formatter.pre_format(measured_value['standard_deviation_value'], self._data['display_mask_id'])
                data['rows'][self.uid]['stop_light']['deviation'].append(deviation_value)

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
                data['rows'][self.uid]['average_settings']['data'].append(value)
                data['rows'][self.uid]['average_settings']['std_deviation_data'].append(std_deviation_value)

        # set min/max ever line
        data['rows'][self.uid]['min_ever_settings'] = self.get_ever_min_line_for_chart()
        data['rows'][self.uid]['max_ever_settings'] = self.get_ever_max_line_for_chart()

        return data

    def get_last_meas_date(self):
        res = self._db.Query("""SELECT
                        measurement_time
                    FROM metric_measured_value
                WHERE
                    metric_id = %s
                    AND segment_value_id = %s
                ORDER BY measurement_time DESC
                LIMIT 0, 1
                """, (self._data['element_id'], self._segment_value_id))
        if res:
            date = self._db.record[0]
            return date['measurement_time']
        else:
            return None

    def fetch_interval_values(self, end_date, start_date, format_values=True):
        """
        get measured values for selected time interval
        """
        #self.measured_values = []
        measured_values = []
        self._db.Query("""SELECT
                        measurement_time,
                        moving_average_value,
                        metric_measured_value_id,
                        metric_id,
                        measurement_value_%s AS measurement_value,
                        standard_deviation_value,
                        stoplight_bad_threshold_value,
                        stoplight_good_threshold_value
                    FROM metric_measured_value
                WHERE
                    metric_id = %%s
                    AND metric_measured_value.measurement_time <= %%s
                    AND metric_measured_value.measurement_time >= %%s
                    AND segment_value_id = %%s
                ORDER BY measurement_time
                """ % self.metric_value_type, (self._data['element_id'], str(end_date), str(start_date), self._segment_value_id))
        for measured_value in self._db.record:
            # filter min/max value to chart
            measured_value['measurement_value'] = self.filter_min_max_to_chart(measured_value['measurement_value'])
            if format_values:
                # format value
                measured_value['formatted_measurement_value'] = self.formatter.format_full(measured_value['measurement_value'], self._data['display_mask_id'])
                # format good/bad stop light values
                measured_value['formatted_stoplight_bad_threshold_value'] = self.formatter.format_full(measured_value['stoplight_bad_threshold_value'], self._data['display_mask_id'])
                measured_value['formatted_stoplight_good_threshold_value'] = self.formatter.format_full(measured_value['stoplight_good_threshold_value'], self._data['display_mask_id'])
            #self.measured_values.append(measured_value)
            measured_values.append(measured_value)
        return measured_values

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
            return "%s - %s" % (self._data['name'], charting_interval['chart_title_display_suffix'])
        return self._data['name']

    def get_compare_line_values(self, compare_line, dates, fiscal_period_type, fiscal_header):
        """
        get measured values for selected compare line
        """
        compare_line_dict = {
            'label': compare_line['line_display_name'],
            'data': [],
            'line_width': compare_line['line_width'],
            'line_color': compare_line['line_color'],
            'line_type': compare_line['line_type'],
            'show_pct_change_ind': compare_line['show_pct_change_ind'],
            'highlight_interval_ind': compare_line['highlight_interval_ind'],
            'pct_change_label': compare_line['pct_change_label']
        }

        for i, date in enumerate(dates):
            res = None
            if fiscal_period_type and compare_line['compare_mechanism'] == 'fiscal':
                # fiscal date compare
                if fiscal_header[i]:
                    res_f = None
                    if compare_line['fiscal_compare_type'] == 'prior period':
                        # prior period
                        res_f = self._db.Query("""SELECT * FROM fiscal_%s WHERE fiscal_%s_id < %%s ORDER BY fiscal_%s_id DESC LIMIT 0, 1""" %
                                             (fiscal_period_type, fiscal_period_type, fiscal_period_type), (fiscal_header[i]['fiscal_%s_id' % fiscal_period_type]))
                    elif compare_line['fiscal_compare_type'] == 'prior year':
                        # prior year
                        if fiscal_period_type == 'year':
                            # for year
                            res_f = self._db.Query("""SELECT * FROM fiscal_year WHERE fiscal_year_id < %s ORDER BY fiscal_year_id DESC LIMIT 0, 1""" ,
                                    (fiscal_header[i]['fiscal_year_id']))
                        elif fiscal_period_type == 'quarter':
                            # for quarter
                            res_f = self._db.Query("""SELECT * FROM fiscal_quarter WHERE fiscal_quarter_id < %s AND fiscal_year_id < %s
                                AND quarter_number = %s ORDER BY fiscal_quarter_id DESC LIMIT 0, 1""" ,
                                    (fiscal_header[i]['fiscal_quarter_id'], fiscal_header[i]['fiscal_year_id'], fiscal_header[i]['quarter_number']))
                        elif fiscal_period_type == 'month':
                            # for month
                            res_f = self._db.Query("""SELECT * FROM fiscal_month WHERE fiscal_month_id = %s """ ,
                                (fiscal_header[i]['last_year_fiscal_month_id'], ))
                    if res_f:
                        # if compare fiscal date exists
                        fiscal_date = self._db.record[0]
                        res = self._db.Query("""SELECT metric_measured_value.*,
                                    measurement_value_%s AS measurement_value
                                FROM metric_measured_value
                            WHERE
                                metric_id = %%s
                            AND metric_measured_value.measurement_time >= %%s
                            AND metric_measured_value.measurement_time <= %%s
                            AND segment_value_id = %%s
                            ORDER BY measurement_time ASC
                            LIMIT 0, 1
                            """ % self.metric_value_type,
                                (self._data['element_id'], fiscal_date['first_day_of_period'], fiscal_date['last_day_of_period'],
                                 self._segment_value_id))

            elif compare_line['compare_mechanism'] == 'calendar':
                # calendar date compare
                res = self._db.Query("""SELECT metric_measured_value.*,
                                measurement_value_%s AS measurement_value
                            FROM metric_measured_value
                        WHERE
                            metric_id = %%s
                        AND metric_measured_value.measurement_time = DATE_SUB(%%s, INTERVAL %s %s)
                        AND segment_value_id = %%s
                        """ % (self.metric_value_type, str(compare_line['compare_interval_value']), str(compare_line['compare_interval_unit'])),
                        (self._data['element_id'], date, self._segment_value_id))
            if res:
                row = self._db.record[0]
                row['measurement_value'] = self.filter_min_max_to_chart(row['measurement_value'])
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

    def get_all_annotations(self, header, orig_header, values, metric_order):
        """
        get range and point annotations
        """
        all_annotations = []
        # get point annotations list
        point_annotations = self._get_annotations(header, orig_header)

        # get range annotations list
        range_annotations = self._get_range_annotations(orig_header, values)

        # add to single list
        for i in point_annotations.keys():
            all_annotations.append({'time': orig_header[i],
                                    'is_range': 0,
                                    'index': None,
                                    'header_index': i,
                                    'metric_id': self._id,
                                    'uid': self.uid,
                                    'metric_order': metric_order,
                                    'data': point_annotations[i]})
        # add to single list
        for i, range_annotation in enumerate(range_annotations):
            all_annotations.append({'time': range_annotation['annotation_measurement_start_time'],
                                    'is_range': 1,
                                    'index': None,
                                    'header_index': None,
                                    'metric_id': self._id,
                                    'uid': self.uid,
                                    'metric_order': metric_order,
                                    'data': range_annotation})
        return all_annotations

    def parse_annotations(self, all_annotations, header_len):
        """
        divide all annotations list into range/point annotations lists
        """
        self.annotations = []
        self.range_annotations = []

        #point_annotations = [False for i in xrange(header_len + 1)]
        point_annotations = [False] * header_len

        for annotation in all_annotations:
            if self.uid == annotation['uid']:
                if annotation['is_range']:
                    annotation['data']['index'] = annotation['index']
                    self.range_annotations.append(annotation['data'])
                else:
                    point_annotations[annotation['header_index']] = annotation['index']
                    for point_annotation in annotation['data']:
                        point_annotation['index'] = annotation['index']
                        self.annotations.append(point_annotation)

        return point_annotations, self.range_annotations

    def process_interval(self, charting_interval, end_date, start_date, x_scale_values, xtd_interval):
        """
        process selected charting interval data
        """

        # fetching measured values
        self.measured_values = self.fetch_interval_values(end_date, start_date)

#        # if metric is expired
        #if self._expired_date and (not self.measured_values or self._expired_date > self.measured_values[-1]['measurement_time']):
        if self._expired_date:
            self.spread_to_expired_date(start_date, x_scale_values)

        # get list of compare lines
        compare_lines = self.fetch_compare_lines(charting_interval)
        #create empty data dict
        data = self.init_charting_data(compare_lines, xtd_interval)
        data['thin_by_metric_id'] = self.uid

        data['expired_date'] = self._expired_date

        #get original dates
        orig_headers = self.fetch_orig_headers()

        #create formatted header
        data = self.create_headers(data, orig_headers, x_scale_values, charting_interval['fiscal_period_type'])

        # create dict for passing to chart
        data = self.prepare_measured_values(data)

        data['primary_y_axis_display_mask'] = self.get_y_axis_format()

        # get all annotations
        all_annotations = self.get_all_annotations(data['header'], data['orig_header'], data['rows'][self.uid]['data_settings']['data'], 0)
        # index annotations
        indexed_all_annotations = self.index_annotations(all_annotations)
        # divide all annotations into point/range lists
        data['point_annotations'][self.uid], data['range_annotations'][self.uid] = self.parse_annotations(indexed_all_annotations, len(data['header']))

        for compare_line in compare_lines:
            data['rows'][self.uid]['compare_settings_%s' % (compare_line['compare_line_id'])] = self.get_compare_line_values(compare_line, data['orig_header'], charting_interval['fiscal_period_type'], data['fiscal_header'])

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
                        measurement_value_%s AS measurement_value
                    FROM metric_measured_value
                WHERE
                    metric_id = %%s
                AND metric_measured_value.measurement_time <= %%s
                AND metric_measured_value.measurement_time >= DATE_SUB(%%s, INTERVAL %s)
                AND segment_value_id = %%s""" % (self.metric_value_type, interval),
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
                        metric_max_value_%s AS metric_max_value,
                        metric_min_value_%s AS metric_min_value
                    FROM last_dashboard_element_segment_value
                WHERE
                    element_id = %%s
                AND segment_value_id = %%s""" % (self.metric_value_type, self.metric_value_type),
                (self._data['element_id'], self._segment_value_id ))
        if res:
            self.min_max_ever = self._db.record[0]

            # min/max value to chart
            #min_max_condition = ''
            if self._data['metric_min_value_to_chart'] is not None:
                min_condition = ' AND measurement_value_'+self.metric_value_type+'>='+self._db.escape_string(repr(self._data['metric_min_value_to_chart']))
            else:
                min_condition = ''
            if self._data['metric_max_value_to_chart'] is not None:
                max_condition = ' AND measurement_value_'+self.metric_value_type+'<='+self._db.escape_string(repr(self._data['metric_max_value_to_chart']))
            else:
                max_condition = ''

            # let's check, if max/min values are correct (they may be deleted)
            # check max ever
            if self.min_max_ever['metric_max_value'] is not None:
                res = self._db.Query("""SELECT measurement_value_%s AS measurement_value
                                            FROM metric_measured_value
                                        WHERE 1
                                            AND metric_id = %%s
                                            AND segment_value_id = %%s
                                            AND measurement_time = %%s
                                        LIMIT 0, 1""" % self.metric_value_type,
                                     (self._data['element_id'], self._segment_value_id, self.min_max_ever['metric_max_value_time']))
                if res:
                    max_ever = self._db.record[0]
                    # check if max measured value is still there
                    if max_ever['measurement_value'] != self.min_max_ever['metric_max_value']:
                        self.min_max_ever['metric_max_value'] = None
                else:
                    self.min_max_ever['metric_max_value'] = None

            # check min ever
            if self.min_max_ever['metric_min_value'] is not None:
                res = self._db.Query("""SELECT measurement_value_%s AS measurement_value
                                            FROM metric_measured_value
                                        WHERE 1
                                            AND metric_id = %%s
                                            AND segment_value_id = %%s
                                            AND measurement_time = %%s
                                        LIMIT 0, 1""" % self.metric_value_type,
                                     (self._data['element_id'], self._segment_value_id, self.min_max_ever['metric_min_value_time']))
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
                res = self._db.Query("""SELECT measurement_value_%s AS measurement_value,
                                                measurement_time
                                        FROM metric_measured_value
                                            WHERE 1
                                            %s %s
                                            AND metric_id = %%s
                                            AND segment_value_id = %%s
                                        ORDER BY measurement_value DESC
                                        LIMIT 0, 1""" % (self.metric_value_type, min_condition, max_condition),
                                     (self._data['element_id'], self._segment_value_id))
                if res:
                    max_ever = self._db.record[0]
                    max_ever['formatted_measurement_value'] = self.formatter.format_full(max_ever['measurement_value'], self._data['display_mask_id'])
                    self.update_min_max_ever('max', max_ever)

            # if there is no min ever, let's create it
            if self.min_max_ever['metric_min_value'] is None:
                res = self._db.Query("""SELECT measurement_value_%s AS measurement_value,
                                                measurement_time
                                        FROM metric_measured_value
                                            WHERE 1
                                            %s %s
                                            AND metric_id = %%s
                                            AND segment_value_id = %%s
                                        ORDER BY measurement_value ASC
                                        LIMIT 0, 1""" % (self.metric_value_type, min_condition, max_condition),
                                     (self._data['element_id'], self._segment_value_id))
                if res:
                    min_ever = self._db.record[0]
                    min_ever['formatted_measurement_value'] = self.formatter.format_full(min_ever['measurement_value'], self._data['display_mask_id'])
                    self.update_min_max_ever('min', min_ever)
        else:
            self.min_max_ever = {'metric_max_value': None, 'metric_min_value': None}

    def get_ever_min_line_for_chart(self):
        """
        return min ever if it should be shown in chart
        """
        ever_value = None
        if self.min_max_ever['metric_min_value'] is not None and self._data['metric_show_min_ever_on_chart_ind'] == 'Y':
            ever_value = {'label': 'Minimum Ever', 'data': []}
            ever_value['data'].append(self.min_max_ever['metric_min_value'])
        return ever_value

    def get_ever_max_line_for_chart(self):
        """
        return max ever if it should be shown in chart
        """
        ever_value = None
        if self.min_max_ever['metric_max_value'] is not None and self._data['metric_show_max_ever_on_chart_ind'] == 'Y':
            ever_value = {'label': 'Maximum Ever', 'data': []}
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
                if (self.min_max_ever['metric_min_value'] is None
                        or (self.min_max_ever['metric_min_value'] is not None
                        and value['measurement_value'] < self.min_max_ever['metric_min_value'])):
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
                if (self.min_max_ever['metric_max_value'] is None
                        or (self.min_max_ever['metric_max_value'] is not None
                        and value['measurement_value'] > self.min_max_ever['metric_max_value'])):
                    self.update_min_max_ever('max', value)
                return {'value': value['measurement_value'], 'measurement_time': value['measurement_time']}
        return None

    def update_min_max_ever(self, min_max, value):
        """
        update min / max metric measured values for segment value id
        """
        value['formatted_measurement_time'] = self.formatter.format_date(value['measurement_time'])
        self._db.Query("""UPDATE last_dashboard_element_segment_value
                            SET metric_%s_value_%s = %%s,
                                metric_%s_value_formatted = %%s,
                                metric_%s_value_time = %%s,
                                metric_%s_value_time_formatted = %%s
                            WHERE element_id = %%s
                                AND segment_value_id = %%s""" % (min_max, self.metric_value_type, min_max, min_max, min_max),
                       (value['measurement_value'],
                       value['formatted_measurement_value'],
                       value['measurement_time'],
                       value['formatted_measurement_time'],
                       self._id,
                       self._segment_value_id))

        self.min_max_ever['metric_%s_value' % min_max] = value['measurement_value']
        self.min_max_ever['metric_%s_value_formatted' % min_max] = value['formatted_measurement_value']
        self.min_max_ever['metric_%s_value_time' % min_max] = value['measurement_time']
        self.min_max_ever['metric_%s_value_time_formatted' % min_max] = value['formatted_measurement_time']

    def _get_curr_value(self):
        """
        get metric current measured value
        """
        value = None

        # min/max value to chart
        min_max_condition = ''
        if self._data['metric_min_value_to_chart'] is not None:
            min_max_condition += ' AND measurement_value_%s >= %s' % (self.metric_value_type, self._db.escape_string(repr(self._data['metric_min_value_to_chart'])))
        if self._data['metric_max_value_to_chart'] is not None:
            min_max_condition += ' AND measurement_value_%s <= %s ' % (self.metric_value_type, self._db.escape_string(repr(self._data['metric_max_value_to_chart'])))

        res = self._db.Query("""SELECT metric_measured_value.*,
                        measurement_value_%s AS measurement_value
                    FROM metric_measured_value
                WHERE 1
                    %s
                    AND metric_id = %%s
                    AND measurement_value_%s IS NOT NULL
                    AND segment_value_id = %%s
                ORDER BY measurement_time DESC
                LIMIT 0, 1""" % (self.metric_value_type, min_max_condition, self.metric_value_type),
                (self._data['element_id'], self._segment_value_id))

        if res:
            extreme_value = self._db.record[0]
            value = extreme_value['measurement_value']
            value = self.formatter.pre_format(value, self._data['display_mask_id'])

        return value

    def prepare_min_max(self, metric_value):
        """
        return dict with formatted min/max values and formatted time of min/max values
        """
        if self.min_max_ever['metric_min_value'] is not None:
            metric_value['min_value'] = self.min_max_ever['metric_min_value_formatted']
            metric_value['min_reached_on'] = self.min_max_ever['metric_min_value_time_formatted']
        else:
            metric_value['min_value'] = ''
            metric_value['min_reached_on'] = ''
        if self.min_max_ever['metric_max_value'] is not None:
            metric_value['max_value'] = self.min_max_ever['metric_max_value_formatted']
            metric_value['max_reached_on'] = self.min_max_ever['metric_max_value_time_formatted']
        else:
            metric_value['max_value'] = ''
            metric_value['max_reached_on'] = ''
        return metric_value

    def prepare_metric(self, metrics):
        """
        create metric dict main meta file
        """
        #metrics = []
        metric_value = {
            'metric_element_id': self._data['element_id'],
            'segment_value_id': self._segment_value_id,
            'segment_value': self._segment_value,
            'segment': self._segment,
            'metric_name': self._get_metric_label(),#self._data['name'],
            'pure_metric_name': self._data['name'],
            'metric_descr': self._data['description'],
            'metric_dashboard_category': self._data['category'],
            'metric_primary_topic': self._data['topic_name'],
            'metric_business_owner': self._data['business_owner'],
            'metric_tech_owner': self._data['technical_owner'],
            'metric_interval_id': self._data['measurement_interval_id'],
            'metric_moving_average_interval': self._data['metric_moving_average_interval'],
            'curr_value': self.formatter.format_full(self._get_curr_value(), self._data['display_mask_id']),
            'compare_to': ''
        }

        metric_value = self.prepare_min_max(metric_value)
        metrics.append(metric_value)
        return metrics

    def _get_range_annotations(self, header, values):
        range_annotations = []

        if not header:
            return []

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
                                ORDER BY annotation_measurement_start_time
                                """, (self._id, self._segment_value_id, finish_time, start_time))

        for ann in self._db.record:
            ann['metric_id'] = self._id
            ann['uid'] = self.uid
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
                if d and ann['from_time'] < d:
                    break
                from_ind = i

            to_ind = 0
            for i, d in enumerate(header):
                to_ind = i
                if d and ann['to_time'] <= d:
                    break
            to_ind += 1

            value_line = values[from_ind:to_ind]

            if value_line:
                value = max(value_line)

            if value is None and values:
                value = max(values)

            if value is not None:
                ann['index'] = None
                ann['value'] = value
                range_annotations.append(ann)
        return range_annotations

    def _get_annotations(self, header, orig_header):
        """
        get annotations for specified period
        """
        # metric measured values list with annotation index
        metric_annotations = {}
        ids = []
        real_values = {}

        for measured_value in self.measured_values:
            if measured_value['metric_measured_value_id']:
                ids.append(measured_value['metric_measured_value_id'])
                real_values[measured_value['metric_measured_value_id']] = measured_value

        format_strings = ','.join(['%s'] * len(ids))
        param = list(ids)
        annotations = {}
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
                if ann['metric_instance_id'] not in annotations:
                    annotations[ann['metric_instance_id']] = []
                annotations[ann['metric_instance_id']].append(ann)

        for i, measured_value in enumerate(self.measured_values):
            if measured_value['metric_measured_value_id'] in annotations:
                for j, ann in enumerate(annotations[measured_value['metric_measured_value_id']]):
                    annotations[measured_value['metric_measured_value_id']][j]['measurement_value'] = measured_value['formatted_measurement_value']
                    annotations[measured_value['metric_measured_value_id']][j]['metric_id'] = self._id
                    annotations[measured_value['metric_measured_value_id']][j]['uid'] = self.uid
                    annotations[measured_value['metric_measured_value_id']][j]['index'] = None
                    annotations[measured_value['metric_measured_value_id']][j]['meas_index'] = header[i]
                    annotations[measured_value['metric_measured_value_id']][j]['raw_meas_index'] = orig_header[i]
                metric_annotations[i] = annotations[measured_value['metric_measured_value_id']]
        return metric_annotations

    def _prepare_map(self, map, prepared_data):
        """
        parse map data received from chart generator
        """
        prepared_map = []

        if self.uid not in map['data']:
            return prepared_map
        for i, date in enumerate(prepared_data['header']):
            prepared_map_item = OrderedDict()

            # get measured value coordinates
            if i in map['data'][self.uid]:
                row = map['data'][self.uid][i]
                prepared_map_item['shape'] = row['shape']
                prepared_map_item['draw_shape_order'] = row['draw_shape_order']
                prepared_map_item['coords'] = row['coords']
                prepared_map_item['metric_id'] = self._id
                prepared_map_item['metric_name'] = prepared_data['rows'][self.uid]['data_settings']['label']#self._data['name']
                prepared_map_item['metric_instance_id'] = self.measured_values[i]['metric_measured_value_id']
                prepared_map_item['meas_time'] = date
                prepared_map_item['raw_meas_time'] = str(self.measured_values[i]['measurement_time'])
                prepared_map_item['value'] = self.measured_values[i]['formatted_measurement_value']
                prepared_map_item['raw_value'] = self.measured_values[i]['measurement_value']
                prepared_map_item['moving_avg_value'] = ''
                prepared_map_item['raw_moving_avg_value'] = ''
                prepared_map_item['standard_deviation_value'] = ''
                prepared_map_item['raw_standard_deviation_value'] = ''
                if self._show_std_deviation or self._show_moving_average:
                    prepared_map_item['raw_moving_avg_value'] = prepared_data['rows'][self.uid]['average_settings']['data'][i]
                    prepared_map_item['moving_avg_value'] = self.formatter.format_full(prepared_data['rows'][self.uid]['average_settings']['data'][i], self._data['display_mask_id'])

                    if self._show_moving_average:
                        prepared_map_item['moving average period'] = self._data['metric_moving_average_interval']
                    else:
                        prepared_map_item['moving average period'] = self._data['metric_std_deviation_moving_average_interval']

                    if self._show_std_deviation:
                        prepared_map_item['raw_standard_deviation_value'] = self.measured_values[i]['standard_deviation_value']
                        prepared_map_item['standard_deviation_value'] = self.formatter.format_full(self.measured_values[i]['standard_deviation_value'], self._data['display_mask_id'])

                prepared_map_item['compares'] = []

                for compare_line in prepared_data['compare_lines']:
                    compare_id = 'compare_settings_%s' % compare_line
                    compare_row = OrderedDict()
                    compare_row['disp_line_name'] = prepared_data['rows'][self.uid][compare_id]['label']
                    compare_row['line_value'] = self.formatter.format_full(prepared_data['rows'][self.uid][compare_id]['data'][i], self._data['display_mask_id'])
                    compare_row['raw_value'] = ''
                    compare_row['pct_change_label'] = ''

                    if prepared_data['rows'][self.uid][compare_id]['show_pct_change_ind'] == 'Y':
                        compare_row['raw_value'] = str(prepared_data['rows'][self.uid][compare_id]['data'][i])
                        if prepared_data['rows'][self.uid][compare_id]['pct_change_label']:
                            compare_row['pct_change_label'] = prepared_data['rows'][self.uid][compare_id]['pct_change_label']

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
                            if (self.measured_values[i]['stoplight_good_threshold_value'] is not None
                                    and self.measured_values[i]['measurement_value'] >= self.measured_values[i]['stoplight_good_threshold_value']):
                                prepared_map_item['stoplight_variance'] = self.formatter.format_full(abs(self.measured_values[i]['measurement_value'] - self.measured_values[i]['stoplight_good_threshold_value']), self._data['display_mask_id'])
                            elif (self.measured_values[i]['stoplight_bad_threshold_value'] is not None
                                  and self.measured_values[i]['measurement_value'] <= self.measured_values[i]['stoplight_bad_threshold_value']):
                                prepared_map_item['stoplight_variance'] = self.formatter.format_full(abs(self.measured_values[i]['stoplight_bad_threshold_value'] - self.measured_values[i]['measurement_value']), self._data['display_mask_id'])
                    else:
                        if self.measured_values[i]['measurement_value'] is not None:
                            if (self.measured_values[i]['stoplight_good_threshold_value'] is not None
                                    and self.measured_values[i]['measurement_value'] <= self.measured_values[i]['stoplight_good_threshold_value']):
                                prepared_map_item['stoplight_variance'] = self.formatter.format_full(abs(self.measured_values[i]['measurement_value'] - self.measured_values[i]['stoplight_good_threshold_value']), self._data['display_mask_id'])
                            elif (self.measured_values[i]['stoplight_bad_threshold_value'] is not None
                                  and self.measured_values[i]['measurement_value'] >= self.measured_values[i]['stoplight_bad_threshold_value']):
                                prepared_map_item['stoplight_variance'] = self.formatter.format_full(abs(self.measured_values[i]['stoplight_bad_threshold_value'] - self.measured_values[i]['measurement_value']), self._data['display_mask_id'])
                prepared_map.append(prepared_map_item)

        for i, annot in enumerate(self.annotations):
            self.annotations[i]['coords'] = map['point_annotations'][annot['index']]['coords']
            self.annotations[i]['shape'] = map['point_annotations'][annot['index']]['shape']
            self.annotations[i]['annotation_interval'] = 'point'
            self.annotations[i]['start_time'] = ''
            self.annotations[i]['finish_time'] = ''
            self.annotations[i]['raw_start_time'] = ''
            self.annotations[i]['raw_finish_time'] = ''

        for i, annot in enumerate(self.range_annotations):
            self.range_annotations[i]['meas_index'] = ''
            self.range_annotations[i]['raw_meas_index'] = ''
            self.range_annotations[i]['coords'] = map['range_annotations'][annot['index']]['coords']
            self.range_annotations[i]['shape'] = map['range_annotations'][annot['index']]['shape']
            self.range_annotations[i]['annotation_interval'] = 'range'
            self.range_annotations[i]['raw_start_time'] = self.range_annotations[i]['annotation_measurement_start_time']
            self.range_annotations[i]['raw_finish_time'] = self.range_annotations[i]['annotation_measurement_finish_time']
            self.range_annotations[i]['start_time'] = self.formatter.format_date(self.range_annotations[i]['annotation_measurement_start_time'])
            self.range_annotations[i]['finish_time'] = self.formatter.format_date(self.range_annotations[i]['annotation_measurement_finish_time'])
        self.annotations.extend(self.range_annotations)

        return prepared_map

    def _set_current_segment(self, segment_value):
        """
        set current segment value
        """
        if segment_value:
            self._segment_value_id = segment_value['segment_value_id']
            self._segment_value = segment_value
        else:
            self._segment_value_id = 0
            self._segment_value = None

    def process_chart_interval(self, charting_interval):
        """
        draw charts for specified charting interval
        """
        self.formatter.set_def_date_format_id(charting_interval['display_mask_id'])

        # get period for fetching measured values
        end_date, start_date, scale_values, xtd_interval = self.get_interval(charting_interval)

        # prepare data for charting
        data = self.process_interval(charting_interval, end_date, start_date, scale_values, xtd_interval)

        # draw chart, get image map data
        if (not self.index_interval_only) and charting_interval['index_charting_interval_ind'] == 'Y':
            is_index = True
        else:
            is_index = False

        metric_chart = MetricChart(self._id, charting_interval['charting_interval_id'], data, self._jfile, 'large', is_index, self.formatter)
        orig_map = metric_chart.generate_chart()

        # fetch image map data for json
        map = self._prepare_map(orig_map, data)

        # write json
        self._jfile.make_chart_interval(map, charting_interval['charting_interval_id'], charting_interval, self.annotations)

        #create thumbnail/preview if it's not index interval only mode
        if is_index:
            thumb_data = copy.deepcopy(data)
            metric_chart = MetricChart(self._id, charting_interval['charting_interval_id'], thumb_data, self._jfile, 'thumbnail', False, self.formatter)
            metric_chart.generate_chart()

            data['header'] = [self.preview_formatter.format_date(date) for date in data['orig_header']]
            data['x_scale_labels'] = [self.preview_formatter.format_date(date) for date in data['x_scale_values']]

            metric_chart = MetricChart(self._id, charting_interval['charting_interval_id'], data, self._jfile, 'preview', False, self.formatter)
            metric_chart.generate_chart()

    def _get_non_empty_segments(self):
        """
        Filters segment values with some data only
        """
        segments = []
        for segment in self._segment_values:
            if self._fetch_last_meas_time(segment['segment_value_id']):
                segments.append(segment)
        return segments

    def _fetch_last_meas_time(self, segment_value_id):
        """
        Fetches last measurement time from db
        """
        last_measurement_time = None
        res = self._db.Query("""SELECT last_measurement_time
                            FROM last_dashboard_element_segment_value
                        WHERE
                            element_id = %s
                            AND segment_value_id = %s
                        """,(self._id, segment_value_id))
        if res:
            item = self._db.record[0]
            last_measurement_time = item['last_measurement_time']
        return last_measurement_time

    def _check_interval_has_data(self, charting_interval):
        # get period for fetching measured values
        end_date, start_date = self.get_interval_end_start_dates(charting_interval)
        measured_values = self.fetch_interval_values(end_date, start_date, format_values=False)
        if measured_values:
            return any([measured_value['measurement_value'] is not None for measured_value in measured_values])

        return False

    def _get_non_empty_charting_intervals(self):
        available_charting_intervals = []
        for charting_interval in self._charting_intervals:
            if self._check_interval_has_data(charting_interval):
                available_charting_intervals.append(charting_interval)
        
        return available_charting_intervals
