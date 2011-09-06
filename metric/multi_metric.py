# -*- coding: utf-8 -*-
from abstract_metric import AbstractMetricElement
from single_metric import MetricElement
from simplejson.ordered_dict import OrderedDict
from chart.metric_chart import MetricChart
from operator import itemgetter
import copy

class MultiMetricElement(AbstractMetricElement):
    """
    Multi metric dashboard element
    """
    #_metrics = OrderedDict()
    _metrics = []
    stoplight_element_id = 0
    thin_by_uid = None

    def __init__(self, id, index_interval_only):
        AbstractMetricElement.__init__(self, id, index_interval_only)
        self._type = 'multimetric'
        self.thin_by_uid = None

        # json file operation wrapper
        self._path = self.config.multimetric_root


    def init(self):
        """
        init multi metric and get all sub metrics
        """
        AbstractMetricElement.init(self)
        self._metrics = self._get_metrics()

    def _get_metrics(self):
        """
        return list of single metrics and set specific settings for each metric
        """
        #metrics = OrderedDict()
        metrics = []
        if self._data['segment_id']:
            #TODO: remove "GROUP BY dashboard_element.`element_id`"
            res = self._db.Query("""SELECT chart_metric.*, dashboard_element.`element_id`
                                FROM chart_metric, dashboard_element
                            WHERE
                                chart_metric.chart_element_id = %s
                            AND
                                dashboard_element.element_id = chart_metric.metric_element_id
                            AND
                                (IFNULL(dashboard_element.`segment_id`,0) = %s OR IFNULL(dashboard_element.`segment_id`,0) = 0)
                            GROUP BY dashboard_element.`element_id`
                            ORDER BY charting_order""",(self._id, self._data['segment_id']))
        else:
            res = self._db.Query("""SELECT chart_metric.*, dashboard_element.`element_id`
                                FROM chart_metric, dashboard_element
                            WHERE
                                chart_metric.chart_element_id = %s
                            AND
                                dashboard_element.element_id = chart_metric.metric_element_id
                            ORDER BY charting_order""",(self._id, ))
        if not res:
            raise Exception("No single metrics for multi-metric element %s (%s) available" % (self._data['name'], self._id))

        uid = 0
        any_unsegmented = False

        for row in self._db.record:
            metric = MetricElement(row['element_id'], self.index_interval_only)
            metric.sub_settings = row
            metric.uid = uid

            if not self._data['segment_id'] and metric._data['segment_id'] and not row['segment_value_id']:
                raise Exception("No segment specified for segmented metric '%s' in non-segmented multi metric" % (metric._data['name'], ))

            uid += 1
            metric._data['include_metric_in_thumbnail_ind'] = row['include_metric_in_thumbnail_ind']
            metric._data['measurement_interval_id'] = self._data['measurement_interval_id']

            if self.stoplight_element_id and metric._id == self.stoplight_element_id:
                self.thin_by_uid = metric.uid
                self._data['show_expired_zone_ind'] = metric._data['show_expired_zone_ind']
                self._data['max_time_before_expired_sec'] = metric._data['max_time_before_expired_sec']
            else:
                metric._data['show_expired_zone_ind'] = 'N'

            if metric._data['metric_display_label']:
                metric._data['name'] = metric._data['metric_display_label']

            if not metric._data['segment_id']:
                any_unsegmented = True

            if row['include_moving_average_line_ind'] == 'Y':
                if row['metric_chart_display_type'] == 'line':
                    type = 'line'
                else:
                    type = 'bar'

                res = self._db.Query("""SELECT *
                            FROM chart_layout_dataset
                        WHERE
                            chart_layout_dataset.layout_id = %%s
                            AND %s_color = %%s
                        """ % (type, ),(self._data['chart_layout_id'], row['metric_color']))
                if not res:
                    res = self._db.Query("""SELECT *
                            FROM chart_layout_dataset
                        WHERE
                            chart_layout_dataset.layout_id = %s
                            AND color = %s
                        """,(self._data['chart_layout_id'], row['metric_color']))
                    if not res:
                        res = self._db.Query("""SELECT *
                            FROM chart_layout_dataset
                        WHERE
                            chart_layout_dataset.layout_id = %s
                        """,(self._data['chart_layout_id']))
                        if not res:
                            raise Exception("Average setting not specified for single metric %s (%s) in multi-metric chart %s (%s)" % (metric._data['name'], metric._id, self._data['name'], self._id))
                moving_average = self._db.record[0]
                metric._data['moving_average_line_color'] = moving_average['moving_average_color']
                metric._data['moving_average_line_type'] = self._data['multi_metric_moving_average_line_type']
                metric._data['moving_average_line_width'] = self._data['multi_metric_moving_average_line_width']
                #metric._data['metric_moving_average_label'] = '%s: Last %s Moving Average' % (metric._data['name'], metric._data['metric_moving_average_interval'])
                metric._show_moving_average = True
                metric._average_with_name = True
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
            metrics.append(metric)


#        if not (self.stoplight_element_id and self.stoplight_element_id in metrics):
#            self.stoplight_element_id = 0
        if self.thin_by_uid is None and metrics:
            self.thin_by_uid = 0

        if (self._data['segment_id'] and any_unsegmented) or not self._data['segment_id']:
            for metric in metrics:
                metric.is_name_with_segment = True

        return metrics

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

    def prepare_metric(self, metrics):
        """
        create metric dict main meta file
        """
        for metric in self._metrics:
            metrics = metric.prepare_metric(metrics)
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
        if self._data['multi_chart_primary_axis_unit_of_measure']:
            y_title = self._data['multi_chart_primary_axis_unit_of_measure']
        else:
            y_title = self._data['name']

        if charting_interval and 'chart_title_display_suffix' in charting_interval:
            return "%s - %s" % (y_title, charting_interval['chart_title_display_suffix'])
        return y_title

    def _get_y2_title(self, charting_interval):
        """
        get y2-axis title
        """
        if self._data['multi_chart_axis_count'] == 2:
            if charting_interval and 'chart_title_display_suffix' in charting_interval:
                return "%s - %s" % (self._data['multi_chart_secondary_axis_unit_of_measure'], charting_interval['chart_title_display_suffix'])
            return self._data['multi_chart_secondary_axis_unit_of_measure']
        return ''

    def get_min_max_ever_from_db(self):
        """
        get minimum/maximum ever dict
        """
        for metric in self._metrics:
            metric.get_min_max_ever_from_db()

    def _set_current_segment(self, segment_value):
        """
        set current segment value
        """
        if segment_value:
            self._segment_value_id = segment_value['segment_value_id']
            self._segment_value = segment_value
            for metric in self._metrics:
                if metric._data['segment_id']:
                    metric._segment_value_id = segment_value['segment_value_id']
                    metric._segment_value = segment_value
                else:
                    metric._segment_value_id = 0
                    metric._segment_value = None
        else:
            self._segment_value_id = 0
            self._segment_value = None
            for metric in self._metrics:
                if metric._data['segment_id']:
                    metric._segment_value_id = metric.sub_settings['segment_value_id']
                    metric._segment_value = [segment_value for segment_value in metric._segment_values if segment_value['segment_value_id'] == metric._segment_value_id][0]
                else:
                    metric._segment_value_id = 0
                    metric._segment_value = None

    def get_last_meas_date(self):
        meas_times = []
        for metric in self._metrics:
            meas_time = metric.get_last_meas_date()
            if meas_time:
                meas_times.append(meas_time)

        if meas_times:
            return max(meas_times)
        return None

    def process_chart_interval(self, charting_interval):
        """
        draw charts for specified charting interval
        """
        self.formatter.set_def_date_format_id(charting_interval['display_mask_id'])

        for metric in self._metrics:
            metric.formatter.set_def_date_format_id(charting_interval['display_mask_id'])

        # get period for fetching measured values
        end_date, start_date, scale_values, xtd_interval = self.get_interval(charting_interval)

        # init dictionary
        data = self.init_charting_data([], xtd_interval)

        data['thin_by_metric_uid'] = self.thin_by_uid

        # all collected headers
        orig_header = []

        #process all metrics
        for metric in self._metrics:
            metric.measured_values = metric.fetch_interval_values(end_date, start_date)
            if self.stoplight_element_id and self.stoplight_element_id == metric._id:
                #if self._expired_date and (not metric.measured_values or self._expired_date > metric.measured_values[-1]['measurement_time']):
                if self._expired_date:
                    metric._expired_date = self._expired_date
                    metric.spread_to_expired_date(start_date, scale_values)
                    self._expired_date = metric._expired_date
                #else:
                #    self._expired_date = None
            # collect common headers for all metrics
            for date in metric.measured_values:
                if date['measurement_time'] not in orig_header:
                    orig_header.append(date['measurement_time'])

        orig_header.sort()
        data = self.create_headers(data, orig_header, scale_values, charting_interval['fiscal_period_type'])

        all_annotations = []
        metric_order = 0

        for metric in self._metrics:
            metric_orig_headers = metric.fetch_orig_headers()
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
                    fake_measured_value = {
                        'measurement_time': date,
                        'moving_average_value': None,
                        'metric_measured_value_id': 0,
                        'metric_id': metric._id,
                        'measurement_value': None,
                        'formatted_measurement_value': ''
                    }

                    #insert fake point
                    metric.measured_values.insert(k, fake_measured_value)
                    metric_orig_headers = metric.fetch_orig_headers()
            data = metric.prepare_measured_values(data, metric.sub_settings['chart_data_point_shape_id'])
            # get annotations
            all_annotations.extend(metric.get_all_annotations(data['header'], data['orig_header'], data['rows'][metric.uid]['data_settings']['data'], metric_order))
            metric_order += 1
        # index point/range annotations
        indexed_all_annotations = self.index_annotations(all_annotations)
        header_len = len(data['header'])

        # divide all annotations into point/range lists
        for metric in self._metrics:
            data['point_annotations'][metric.uid], data['range_annotations'][metric.uid] = metric.parse_annotations(indexed_all_annotations, header_len)

        data['primary_y_axis_display_mask'] = self.get_y_axis_format()
        data['secondary_y_axis_display_mask'] = self.get_y2_axis_format()

        # set titles
        data['x_axis_title'] = self._get_x_title()
        data['y_axis_title_left'] = self._get_y_title(charting_interval)
        data['y_axis_title_right'] = self._get_y2_title(charting_interval)

        data['preview_y_axis_title_left'] = self._data['multi_chart_primary_axis_unit_of_measure']
        data['preview_y_axis_title_right'] = self._data['multi_chart_secondary_axis_unit_of_measure']

        data['y_axis_num'] = self._data['multi_chart_axis_count']

        data['expired_date'] = self._expired_date
        if (not self.index_interval_only) and charting_interval['index_charting_interval_ind'] == 'Y':
            is_index = True
        else:
            is_index = False

        metric_chart = MetricChart(self._id, charting_interval['charting_interval_id'], data, self._jfile, 'large', is_index, self.formatter)
        orig_map = metric_chart.generate_chart()

        map = []
        annotations = []

        # fetch image map data for json
        for metric in self._metrics:
            map.extend(metric._prepare_map(orig_map, data))
            annotations.extend(metric.annotations)

        # resulted points must be sorted by following rule: 1. points (lines) then 2. bars. otherwise bars are overlapping points coordinates
        map = sorted(map, key=itemgetter('draw_shape_order'))

        # write json
        self._jfile.make_chart_interval(map, charting_interval['charting_interval_id'], charting_interval, annotations)

        #create thumbnail/preview if it's not index interval only mode
        if is_index:
            thumb_data = copy.deepcopy(data)
            # remove metrics not included into thumbnail
            for uid, metric in enumerate(self._metrics):
                if metric._data['include_metric_in_thumbnail_ind'] != 'Y':
                    del thumb_data['rows'][uid]

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
        for segment_value in self._segment_values:
            metric_segments = []
            for metric in self._metrics:
                if metric._data['segment_id']:
                    if segment_value:
                        segment_value_id = segment_value['segment_value_id']
                    else:
                        segment_value_id = metric.sub_settings['segment_value_id']
                else:
                    segment_value_id = 0

                if metric._fetch_last_meas_time(segment_value_id):
                    metric_segments.append(True)
                else:
                    metric_segments.append(False)
            if any(metric_segments):
               segments.append(segment_value)

        return segments

    def _get_non_empty_charting_intervals(self):
        available_charting_intervals = []
        for charting_interval in self._charting_intervals:
            metrics_charting_intervals = [metric._check_interval_has_data(charting_interval) for metric in self._metrics]
            if metrics_charting_intervals and any(metrics_charting_intervals):
                available_charting_intervals.append(charting_interval)

        return available_charting_intervals
