#-*- coding: utf-8 -*-

from pychartdir import *
import urllib
import re
from font_manager import FontManager
import time
import datetime
from base_date_chart import AbstractBaseDateChart
from copy import deepcopy
import pprint


class BaseMetricChart(AbstractBaseDateChart):
    inited = False
    interval = None

    def __init__(self, data, jfile, settings):
        AbstractBaseDateChart.__init__(self, data, jfile, settings)
        self._is_x_axis_date = True
        self._has_y_axis = True
        self._prepare_axes_scaling(self.chart_data['orig_header'])

    def set_settings(self, settings):
        self.settings = settings

    def prepare_data(self):
        self.chart_data['split_xtd_periods'] = []
        
        if self.chart_data['xtd_interval'] and self.chart_data['orig_header']:
            xtd_interval = [xtd_date for xtd_date in self.chart_data['xtd_interval'] if xtd_date < self.chart_data['orig_header'][-1] and xtd_date > self.chart_data['orig_header'][0]]

            split_xtd_periods = []
            start = curr = 0
            end = len(self.chart_data['orig_header'])
            for xtd_date in xtd_interval:
                for i, date in enumerate(self.chart_data['orig_header'][start:-1]):
                    if date >= xtd_date:
                        split_xtd_periods.append([start, curr])
                        #curr += 1
                        start = curr
                        break
                    curr += 1
            split_xtd_periods.append([start, end])

            if len(split_xtd_periods) >= 2:
                self.chart_data['split_xtd_periods'] = split_xtd_periods

        #change None to NoValue in the data lists
        for uid, metric_elements in self.chart_data['rows'].iteritems():
            for element_type, element_data in metric_elements.iteritems():
#                if element_type == 'data_settings':
#                    element_data['display_type'] = 'bar'
#                    self.chart_data['rows'][uid][element_type]['display_type'] = 'bar'
                #if element_data and element_data.has_key('axis_number') and element_data['axis_number'] == '2':
                if element_data and 'axis_number' in element_data and element_data['axis_number'] == '2':
                    axis_index = 1
                    self._has_y_axis_2 = True
                else:
                    axis_index = 0
                if element_type == 'max_ever_settings' or element_type == 'min_ever_settings':
                    if element_data is not None:
                        self._has_lines[axis_index] = True
                elif element_type != 'stop_light':
                    if element_data and element_data['display_type'] == 'line':
                        self._has_lines[axis_index] = True
                    elif element_data and element_data['display_type'] == 'bar':
                        self._has_bars[axis_index] += 1
                if element_type == 'stop_light' and element_data is not None:
                    self._charting_values[axis_index].extend([value for value in self.chart_data['rows'][uid]['stop_light']['bad'] if value is not None])
                    self._charting_values[axis_index].extend([value for value in self.chart_data['rows'][uid]['stop_light']['good'] if value is not None])

                    self.chart_data['rows'][uid]['stop_light']['bad'] = map(self._none_to_no_value, self.chart_data['rows'][uid]['stop_light']['bad'])
                    self.chart_data['rows'][uid]['stop_light']['good'] = map(self._none_to_no_value, self.chart_data['rows'][uid]['stop_light']['good'])
                    self.chart_data['rows'][uid]['stop_light']['deviation'] = map(self._none_to_no_value, self.chart_data['rows'][uid]['stop_light']['deviation'])
                else:
                    if element_data is not None:
                        if element_type != 'max_ever_settings' and element_type != 'min_ever_settings':
                            self._charting_values[axis_index].extend([value for value in self.chart_data['rows'][uid][element_type]['data'] if value is not None])
                            if element_type == 'data_settings':
                                if element_data['display_type'] == 'line':
                                    self._line_values[axis_index].append(self.chart_data['rows'][uid][element_type]['data'])
                                elif element_data['display_type'] == 'bar':
                                    self._has_first_bar = True
                                    self._has_last_bar = True

                                if self.layout_data['include_annotations_ind'] == 'Y':
                                    # process range annotations
                                    if self.chart_data['range_annotations'][uid]:
                                        # range annotations
                                        for i, range_annotation in enumerate(self.chart_data['range_annotations'][uid]):
                                            self.chart_data['range_annotations'][uid][i]['from_time_value'] = time.mktime(range_annotation['from_time'].timetuple())
                                            self.chart_data['range_annotations'][uid][i]['to_time_value'] = time.mktime(range_annotation['to_time'].timetuple())
                                            self.chart_data['range_annotations'][uid][i]['middle_time_value'] = (range_annotation['from_time_value'] + range_annotation['to_time_value']) / 2.0
                                            self.chart_data['range_annotations'][uid][i]['middle_time'] = datetime.datetime.fromtimestamp(range_annotation['middle_time_value'])
                                            self._range_annotations_middle_points.append(self.chart_data['range_annotations'][uid][i]['middle_time_value'])

                                            if not range_annotation['right_marker']:
                                                self._has_over_right_border = True
                                            if not range_annotation['left_marker']:
                                                self._has_over_left_border = True


                                    if self.chart_data['point_annotations'][uid]:
                                        # point annotations
                                        # check for annotations for first, last points
                                        if self.chart_data['point_annotations'][uid][0]:
                                            self._has_first_annotation = True
                                        if self.chart_data['point_annotations'][uid][-1]:
                                            self._has_last_annotation = True

                                        #annotations = [self.chart_data['rows'][uid][element_type]['data'][i] if a else None for i, a in enumerate(self.chart_data['point_annotations'][uid])]
                                        #self._annotations[axis_index].append(annotations)

                        self.chart_data['rows'][uid][element_type]['data'] = map(self._none_to_no_value, self.chart_data['rows'][uid][element_type]['data'])

        if self.chart_data['orig_header'] and self.chart_data['orig_header'][0] > self.chart_data['orig_header'][-1]:
            self._has_over_right_border, self._has_over_left_border = self._has_over_left_border, self._has_over_right_border

    def set_interval(self, interval):
        """
        set interval data
        """
        self.interval = interval

    def create_chart(self):
        """
        Main function for creating chart
        """
        self._create_plot_area()

        if not self.inited:
            self._prepare_x_scale_values()
        self._set_x_axis_props()
        self._set_y_axis_props()

        self._set_x_axis_date_scale()

        self._set_y_axes_formats()

        self._create_layers()

        # GOOD area start color
        if self.settings['show_stop_light']:
            # GOOD whisker mark color
            if self.layout_data['metric_stoplight_good_mark_color'] is None:
                self.layout_data['metric_stoplight_good_mark_color'] = 0x00ff00
            else:
                self.layout_data['metric_stoplight_good_mark_color'] = FontManager.get_db_color(self.layout_data['metric_stoplight_good_mark_color'])

            # BAD whisker mark color
            if self.layout_data['metric_stoplight_bad_mark_color'] is None:
                self.layout_data['metric_stoplight_bad_mark_color'] = 0xff0000
            else:
                self.layout_data['metric_stoplight_bad_mark_color'] = FontManager.get_db_color(self.layout_data['metric_stoplight_bad_mark_color'])

            # GOOD area start color
            if self.layout_data['metric_stoplight_good_range_start_color'] is None:
                self.layout_data['metric_stoplight_good_range_start_color'] = 0xa0defacc
            else:
                self.layout_data['metric_stoplight_good_range_start_color'] = FontManager.get_db_color(self.layout_data['metric_stoplight_good_range_start_color'])

            # GOOD area end color
            if self.layout_data['metric_stoplight_good_range_end_color'] is None:
                self.layout_data['metric_stoplight_good_range_end_color'] = 0xa000ff00
            else:
                self.layout_data['metric_stoplight_good_range_end_color'] = FontManager.get_db_color(self.layout_data['metric_stoplight_good_range_end_color'])

            # BAD area start color
            if self.layout_data['metric_stoplight_bad_range_start_color'] is None:
                self.layout_data['metric_stoplight_bad_range_start_color'] = 0xa0f0c9c9
            else:
                self.layout_data['metric_stoplight_bad_range_start_color'] = FontManager.get_db_color(self.layout_data['metric_stoplight_bad_range_start_color'])

            # BAD area end color
            if self.layout_data['metric_stoplight_bad_range_end_color'] is None:
                self.layout_data['metric_stoplight_bad_range_end_color'] = 0xa0ff0000
            else:
                self.layout_data['metric_stoplight_bad_range_end_color'] = FontManager.get_db_color(self.layout_data['metric_stoplight_bad_range_end_color'])

        compare_lines = []
        if 'compare_lines' in self.chart_data and self.chart_data['compare_lines']:
            for compare_line in self.chart_data['compare_lines']:
                compare_lines.append('compare_settings_%s' % compare_line)

        bar_ind = -1

        # iterate metrics. uid: metric uid, _v: metric data
        for uid, metric_elements in self.chart_data['rows'].iteritems():
            # iterate metric elements
            for element_type, element_data in metric_elements.iteritems():
                if element_data is not None:
                    shape = None
                    if element_type == 'max_ever_settings' or element_type == 'min_ever_settings':
                        element_data['line_type'] = self.layout_data['min_max_ever_line_type']
                        element_data['display_type'] = 'line'
                        element_data['color'] = FontManager.get_db_color(self.layout_data['min_max_ever_line_color'])

                    if 'axis_number' in element_data and element_data['axis_number'] == '2':
                        axis = self.c.yAxis2()
                        axis_index = 1
                    else:
                        axis_index = 0
                        axis = self.c.yAxis()

                    # set color and line type dash/dot/solid
                    if 'line_type' not in element_data:
                        element_data['line_type'] = ''
                    color = self._get_element_color(element_data['display_type'], element_data['line_type'], element_data['color'])

                    if element_type == 'max_ever_settings' or element_type == 'min_ever_settings':
                        # draw max/min ever mark
                        element_data['line_type'] = self.layout_data['min_max_ever_line_type']
                        element_data['display_type'] = 'line'
                        element_data['color'] = FontManager.get_db_color(self.layout_data['min_max_ever_line_color'])
                        mark = axis.addMark(
                                            element_data['data'][0],
                                            element_data['color'],
                                            element_data['label'],
                                            FontManager.get_db_font(self.layout_data['min_max_ever_font']),
                                            self.layout_data['min_max_ever_font_size'])
                        mark.setLineWidth(self.layout_data['min_max_ever_line_width'])
                        mark.setAlignment(TopLeft)
                        mark.setDrawOnTop(False)
                    elif element_type == 'stop_light':
                        # draw invisible stop light lines to make this areas visible (included in charting view)
                        if self.chart_data['show_stop_light'] and self.settings['type'] == 'large':
                            self.datasets[self.layers_count] = {}
                            self.datasets[self.layers_count][0] = {'uid': uid, 'metric_data': element_type, 'type': 'good_stop_light', 'shape': ''}
                            self.datasets[self.layers_count][1] = {'uid': uid, 'metric_data': element_type, 'type': 'good_stop_light', 'shape': ''}
                            self.dataset_ids[self.layers_count] = 1
                            self.layers_count += 1

                            stop_light_area_line = self.c.addSplineLayer()
                            stop_light_area_line.setXData(self._x_axis_data)
                            stop_light_area_line.setUseYAxis(axis)
                            stop_light_area_line.addDataSet(element_data['bad'], Transparent)
                            stop_light_area_line.addDataSet(element_data['good'], Transparent)

                            stop_light_type = 'line'

                            # if chart is bar type and show stop light is True
                            if self.chart_data['rows'][uid]['data_settings']['display_type'] == 'bar' and self.settings['show_stop_light']:
                                stop_light_type = 'bar'

                                bad_stop_light_box = self.c.addBoxWhiskerLayer(
                                        None,
                                        None,
                                        None,
                                        None,
                                        element_data['bad'],
                                        -1,
                                        self.layout_data['metric_stoplight_bad_mark_color'])

                                bad_stop_light_box.setLineWidth(2)
                                bad_stop_light_box.setXData(self._x_axis_data)
                                bad_stop_light_box.setUseYAxis(axis)
                                bad_stop_light_box.moveFront()

                                good_stop_light_box = self.c.addBoxWhiskerLayer(
                                        None,
                                        None,
                                        None,
                                        None,
                                        element_data['good'],
                                        -1,
                                        self.layout_data['metric_stoplight_good_mark_color'])
                                good_stop_light_box.setLineWidth(2)
                                good_stop_light_box.setXData(self._x_axis_data)
                                good_stop_light_box.setUseYAxis(axis)
                                good_stop_light_box.moveFront()

                                self.datasets[self.layers_count] = {}
                                self.datasets[self.layers_count][0] = {'uid': uid, 'metric_data': element_type, 'type': 'good_stop_light', 'shape': ''}
                                self.dataset_ids[self.layers_count] = 0
                                self.layers_count += 1

                                self.datasets[self.layers_count] = {}
                                self.datasets[self.layers_count][0] = {'uid': uid, 'metric_data': element_type, 'type': 'good_stop_light', 'shape': ''}
                                self.dataset_ids[self.layers_count] = 0
                                self.layers_count += 1

                            # add title to legend box
                            if self.layout_data['include_legend_ind'] == 'Y' and self.settings['show_stop_light']:
                                if stop_light_type == 'bar':
                                    good_color = self.layout_data['metric_stoplight_good_mark_color']
                                    bad_color = self.layout_data['metric_stoplight_bad_mark_color']

                                    # add GOOD/BAD title to legend box
                                    l_legend = self.legend_layer.addDataSet([], bad_color, u"Bad Level")
                                    l_legend.setDataLabelStyle(self.legend_font, self.legend_font_size, Transparent)
                                    l_legend.setLineWidth(2)
                                    #l_legend.setDataSymbol4(self._cSquareSymbol, 15, bad_color, -1)
                                    l_legend.setDataSymbol(CircleShape, 1, bad_color, bad_color)

                                    l_legend = self.legend_layer.addDataSet([], good_color, u"Good Level")
                                    l_legend.setLineWidth(2)
                                    #l_legend.setDataSymbol4(self._cSquareSymbol, 15, good_color, -1)
                                    l_legend.setDataSymbol(CircleShape, 1, good_color, good_color)
                                else:
                                    good_color = self.layout_data['metric_stoplight_good_range_end_color']
                                    bad_color = self.layout_data['metric_stoplight_bad_range_end_color']

                                    # add GOOD/BAD title to legend box
                                    l_legend = self.legend_layer.addDataSet([], bad_color, u"Bad Area")
                                    l_legend.setDataLabelStyle(self.legend_font, self.legend_font_size, Transparent)
                                    l_legend.setLineWidth(0)
                                    l_legend.setDataSymbol4(self._cSquareSymbol, 15, bad_color, -1)

                                    l_legend = self.legend_layer.addDataSet([], good_color, u"Good Area")
                                    l_legend.setLineWidth(0)
                                    l_legend.setDataSymbol4(self._cSquareSymbol, 15, good_color, -1)
                    else:
                        # draw bar/line chart
                        std_deviation_color = None

                        # hide compare lines for deviation/stoplight chart
                        if element_type in compare_lines and (self.settings['show_std_deviation'] or self.settings['show_stop_light']):
                            color = Transparent
                        # chart average line
                        if element_type == 'average_settings':
                            # hide average line if it should be shown on main chart
                            if not element_data['show_moving_average'] and not self.settings['show_std_deviation']:
                                color = Transparent

                            # chart standard deviation area
                            if element_data['show_std_deviation']:
                                # hide deviation lines for main chart
                                if not self.settings['show_std_deviation']:
                                    # make it absolute transparent
                                    std_deviation_color = Transparent
                                else:
                                    # make it half transparent
                                    std_deviation_color = color + 0xa0000000

                                min_deviation = []
                                max_deviation = []

                                for i, avg in enumerate(element_data['data']):
                                    if avg == NoValue or element_data['std_deviation_data'][i] is None:
                                        min_deviation.append(NoValue)
                                        max_deviation.append(NoValue)
                                    else:
                                        min_deviation.append(avg - element_data['metric_unusual_value_std_dev'] * element_data['std_deviation_data'][i])
                                        max_deviation.append(avg + element_data['metric_unusual_value_std_dev'] * element_data['std_deviation_data'][i])

                                top_line = self.c.addLineLayer(max_deviation, Transparent)
                                top_line.setUseYAxis(axis)
                                top_line.setXData(self._x_axis_data)

                                bottom_line = self.c.addLineLayer(min_deviation, Transparent)
                                bottom_line.setUseYAxis(axis)
                                bottom_line.setXData(self._x_axis_data)

                                inter_layer = self.c.addInterLineLayer(top_line.getLine(), bottom_line.getLine(), std_deviation_color)
                                inter_layer.setUseYAxis(axis)

                                self.datasets[self.layers_count] = {}
                                self.datasets[self.layers_count][0] = {'uid': uid, 'metric_data': element_type, 'type': 'std_deviation_top_line', 'shape': ''}
                                self.dataset_ids[self.layers_count] = 0
                                self.layers_count += 1

                                self.datasets[self.layers_count] = {}
                                self.datasets[self.layers_count][0] = {'uid': uid, 'metric_data': element_type, 'type': 'std_deviation_bottom_line', 'shape': ''}
                                self.dataset_ids[self.layers_count] = 0
                                self.layers_count += 1

                                self.datasets[self.layers_count] = {}
                                self.datasets[self.layers_count][0] = {'uid': uid, 'metric_data': element_type, 'type': 'std_deviation_inter_line_area', 'shape': ''}
                                self.dataset_ids[self.layers_count] = 0
                                self.layers_count += 1

                        if element_data['display_type'] == 'line':
                            # line chart
                            line_description = {'uid': uid, 'metric_data': element_type, 'type': 'line_data', 'shape': 'point'}
                            line_point_description = {'uid': uid, 'metric_data': element_type, 'type': 'data', 'shape': 'point'}
                            if element_type == 'data_settings' and self.layout_data['show_line_data_points_ind'] == 'Y':
                                show_data_points = True
                                shape = self._set_shape(self.chart_data['rows'][uid]['data_settings']['line_point_shape'])
                            else:
                                show_data_points = False
                                shape = None

                            if 'line_width' in element_data:
                                line_width = element_data['line_width']
                            else:
                                line_width = None
                            if self.chart_data['split_xtd_periods']:
                                self._create_line_layer_xtd(element_data['data'], element_data['line_style'], line_width, color, axis, show_data_points, shape, line_description, line_point_description)
                            else:
                                self._create_line_layer(element_data['data'], element_data['line_style'], line_width, color, axis, show_data_points, shape, line_description, line_point_description)

                        else:
                            bar_ind += 1
                            # bar chart
                            self._create_bar_layer(element_data['data'], color, axis, axis_index, bar_ind, True, {'uid': uid, 'metric_data': element_type, 'type': 'data', 'shape': 'bar'})

                        # add layer for annotation markers
                        if element_type == 'data_settings' and self.layout_data['include_annotations_ind'] == 'Y':
                            # process range annotations
                            if not self.inited:
                                # calculate range annotations only if they are not calculated before
                                # this is needed only for charting deviation/stoplight metrics
                                self._collect_range_annotations(
                                        self.chart_data['range_annotations'][uid],
                                        axis_index,
                                        {'uid': uid, 'element_type': element_type})

                            # draw point annotations
                            self._draw_point_annotations(
                                    self.chart_data['point_annotations'][uid],
                                    element_data['data'],
                                    axis,
                                    axis_index,
                                    element_data['display_type'],
                                    bar_ind,
                                    True,
                                    {'uid': uid, 'metric_data': element_type, 'type': 'annotation'})

                        # add element name to legend box
                        if self.layout_data['include_legend_ind'] == 'Y':
                            axis = self.c.yAxis()
                            # do not show legend if compare lines are hidden
                            if element_type in compare_lines and (self.settings['show_std_deviation'] or self.settings['show_stop_light']):
                                continue

                            if element_type == 'average_settings':
                                if not element_data['show_moving_average'] and not self.settings['show_std_deviation']:
                                    continue

                            l_legend = self.legend_layer.addDataSet([], color, element_data['label'])

                            if element_data['display_type'] == 'line':
                                l_legend.setDataSymbol(CircleShape, element_data['line_width'], color, color)

                            l_legend.setUseYAxis(axis)

                            if element_type == 'data_settings':
                                if element_data['display_type'] == 'line':
                                    #if element_data.has_key('line_width'):
                                    if 'line_width' in element_data:
                                        l_legend.setLineWidth(element_data['line_width'])

                                    if self.layout_data['show_line_data_points_ind'] == 'Y':
                                        l_legend.setDataSymbol(shape, self.layout_data['line_data_point_dot_size'], color, color)
                                    else:
                                        l_legend.setDataSymbol(CircleShape, element_data['line_width'], color, color)
                                elif element_data['display_type'] == 'bar':
                                    l_legend.setLineWidth(0)
                                    l_legend.setDataSymbol4(self._cSquareSymbol, 15, color, -1)

                            if element_type == 'average_settings' and self.settings['show_std_deviation'] and element_data['show_std_deviation']:
                                l_legend = self.legend_layer.addDataSet([], color, element_data['std_deviation_label'])
                                l_legend.setLineWidth(0)
                                l_legend.setDataSymbol4(self._cSquareSymbol, 15, std_deviation_color, -1)

        # show expired zone
        if self.chart_data['expired_zone']:
            self.c.xAxis().addZone(chartTime2(float(self.chart_data['expired_zone']['start'].strftime("%s"))), chartTime2(float(self.chart_data['expired_zone']['end'].strftime("%s"))), self.chart_data['expired_zone']['color'])

        if self.layout_data['include_annotations_ind'] == 'Y':
            if self._range_annotations:
                if not self.inited:
                    self._calc_annotations()
                self.draw_range_annotations()

        if not self.inited:
            self.inited = True

        # final scale y axis
        self._set_y_axes_scaling()

        # ensure that plots fit within specified layout dimensions
        self._pack_plot_area()

        # show real stop light ares for line chart
        if self.settings['show_stop_light']:
            #freeze chart
            self.c.layoutAxes()
            #get max value of y-axis
            max_y_val1 = self.c.yAxis().getMaxValue()
            max_y_val2 = self.c.yAxis2().getMaxValue()

            min_y_val1 = self.c.yAxis().getMinValue()
            min_y_val2 = self.c.yAxis2().getMinValue()

            for uid, _v in self.chart_data['rows'].iteritems():
                for element_type, element_data in _v.iteritems():
                    if element_type == 'stop_light':
                        #if element_data.has_key('axis_number') and element_data['axis_number'] == '2':
                        if 'axis_number' in element_data and element_data['axis_number'] == '2':
                            axis = self.c.yAxis2()
                            max_y_val = max_y_val2
                            min_y_val = min_y_val2
                        else:
                            axis = self.c.yAxis()
                            max_y_val = max_y_val1
                            min_y_val = min_y_val1

                        # for line metric draw GOOD/BAD areas
                        if self.chart_data['rows'][uid]['data_settings']['display_type'] == 'line':
                            not_empty_bad = [val for val in element_data['bad'] if val != NoValue]
                            not_empty_good = [val for val in element_data['good'] if val != NoValue]

                            if element_data['metric_more_is_better_ind'] == 'Y':
                                top_area = element_data['good']
                                bottom_area = element_data['bad']

                                top_start_color = self.layout_data['metric_stoplight_good_range_start_color']
                                top_end_color = self.layout_data['metric_stoplight_good_range_end_color']

                                bottom_start_color = self.layout_data['metric_stoplight_bad_range_start_color']
                                bottom_end_color = self.layout_data['metric_stoplight_bad_range_end_color']

                                if not_empty_bad:
                                    bottom_largest = max([val for val in element_data['bad'] if val != NoValue])
                                else:
                                    bottom_largest = min_y_val

                                if not_empty_good:
                                    top_smallest = min([val for val in element_data['good'] if val != NoValue])
                                else:
                                    top_smallest = max_y_val
                            else:
                                top_area = element_data['bad']
                                bottom_area = element_data['good']

                                top_start_color = self.layout_data['metric_stoplight_bad_range_start_color']
                                top_end_color = self.layout_data['metric_stoplight_bad_range_end_color']

                                bottom_start_color = self.layout_data['metric_stoplight_good_range_start_color']
                                bottom_end_color = self.layout_data['metric_stoplight_good_range_end_color']

                                if not_empty_good:
                                    bottom_largest = max([val for val in element_data['good'] if val != NoValue])
                                else:
                                    bottom_largest = min_y_val

                                if not_empty_bad:
                                    top_smallest = max([val for val in element_data['bad'] if val != NoValue])
                                else:
                                    top_smallest = max_y_val

                            # bottom area
                            top_line = self.c.addLineLayer(bottom_area, Transparent)
                            top_line.setUseYAxis(axis)
                            top_line.setXData(self._x_axis_data)

                            bottom_line = self.c.addLineLayer([min_y_val for i in bottom_area], Transparent)
                            bottom_line.setXData(self._x_axis_data)
                            bottom_line.setUseYAxis(axis)

                            top_y = stop_light_area_line.getYCoor(bottom_largest)
                            bottom_y = stop_light_area_line.getYCoor(min_y_val)

                            bottom_color = self.c.linearGradientColor(0,
                                                              bottom_y,
                                                              0,
                                                              top_y,
                                                              bottom_end_color,
                                                              bottom_start_color)

                            bottom_area = self.c.addInterLineLayer(top_line.getLine(), bottom_line.getLine(), bottom_color)
                            bottom_area.setUseYAxis(axis)

                            self.datasets[self.layers_count] = {}
                            self.datasets[self.layers_count][0] = {'uid': uid, 'metric_data': element_type, 'type': 'good_stop_light', 'shape': ''}
                            self.dataset_ids[self.layers_count] = 0
                            self.layers_count += 1

                            self.datasets[self.layers_count] = {}
                            self.datasets[self.layers_count][0] = {'uid': uid, 'metric_data': element_type, 'type': 'good_stop_light', 'shape': ''}
                            self.dataset_ids[self.layers_count] = 0
                            self.layers_count += 1

                            self.datasets[self.layers_count] = {}
                            self.datasets[self.layers_count][0] = {'uid': uid, 'metric_data': element_type, 'type': 'good_stop_light', 'shape': ''}
                            self.dataset_ids[self.layers_count] = 0
                            self.layers_count += 1

                            # top area
                            bottom_line = self.c.addLineLayer(top_area, Transparent)
                            bottom_line.setXData(self._x_axis_data)
                            bottom_line.setUseYAxis(axis)

                            top_line = self.c.addLineLayer([max_y_val for i in top_area], Transparent)
                            top_line.setUseYAxis(axis)
                            top_line.setXData(self._x_axis_data)

                            top_y = stop_light_area_line.getYCoor(max_y_val)
                            bottom_y = stop_light_area_line.getYCoor(top_smallest)

                            top_color = self.c.linearGradientColor(0,
                                                              bottom_y,
                                                              0,
                                                              top_y,
                                                              top_start_color,
                                                              top_end_color)

                            top_area = self.c.addInterLineLayer(top_line.getLine(), bottom_line.getLine(), top_color)
                            top_area.setUseYAxis(axis)

                            self.datasets[self.layers_count] = {}
                            self.datasets[self.layers_count][0] = {'uid': uid, 'metric_data': element_type, 'type': 'good_stop_light', 'shape': ''}
                            self.dataset_ids[self.layers_count] = 0
                            self.layers_count += 1

                            self.datasets[self.layers_count] = {}
                            self.datasets[self.layers_count][0] = {'uid': uid, 'metric_data': element_type, 'type': 'good_stop_light', 'shape': ''}
                            self.dataset_ids[self.layers_count] = 0
                            self.layers_count += 1

                            self.datasets[self.layers_count] = {}
                            self.datasets[self.layers_count][0] = {'uid': uid, 'metric_data': element_type, 'type': 'good_stop_light', 'shape': ''}
                            self.dataset_ids[self.layers_count] = 0
                            self.layers_count += 1

        filename = ''
        if self.settings['type'] == 'large':
            if self.settings['show_stop_light']:
                filename = self._jfile.get_chart_file_name(self.interval, suffix='_stoplight')
            elif self.settings['show_std_deviation']:
                filename = self._jfile.get_chart_file_name(self.interval, suffix='_std_dev')
            else:
                filename = self._jfile.get_chart_file_name(self.interval, suffix='')
        elif self.settings['type'] == 'thumbnail':
            filename = self._jfile.get_thumbnail_file_name()
        elif self.settings['type'] == 'preview':
            filename = self._jfile.get_preview_file_name()

        # draw chart
        if not self.c.makeChart(filename):
            raise Exception("ChartDirector cannot create image file %s." % filename)
        self.file_man.change_perm(filename)

        if self.settings['type'] == 'large' and not (self.settings['show_stop_light'] or self.settings['show_std_deviation']) and self.settings['is_index']:
            self.create_resized_preview()

    def get_parsed_map(self):
        ret_res = {
            'data': {},
            'point_annotations': {},
            'range_annotations': {}
        }

        content = self.c.getHTMLImageMap('url', 'x={x}&dataSet={dataSet}&dataSetName={dataSetName}&value={value}&layerId={layerId}&extra_field={field0}')
        content = urllib.unquote(content)
        image_map = re.findall(r'(<area shape="(.+?)" coords="(.+?)" href="url\?x=(.+?)&dataSet=(.+?)&dataSetName=&value=(.+?)&layerId=(.+?)&extra_field=(.*?)">)', content)

        data_result = list({
                        'shape': v[1],
                        'coords': v[2],
                        'name': v[5],
                        'value': v[5],
                        'belongs_to': self.datasets[int(v[6])][int(v[4])],
                        'index': int(v[7]),
                        'draw_shape_order': 0 if self.datasets[int(v[6])][int(v[4])]['shape'] == 'point' else 1}
            for v in image_map if self.datasets[int(v[6])][int(v[4])]['metric_data'] == 'data_settings' and
                self.datasets[int(v[6])][int(v[4])]['type'] == 'data')

        annotation_result = list({
                        'shape': v[1],
                        'coords': v[2],
                        'name': v[5],
                        'value': v[5],
                        'belongs_to': self.datasets[int(v[6])][int(v[4])],
                        'annot_index': v[7]}
            for v in image_map if self.datasets[int(v[6])][int(v[4])]['metric_data'] == 'data_settings' and
                self.datasets[int(v[6])][int(v[4])]['type'] == 'annotation')

        range_annotation_result = list({
                        'shape': v[1],
                        'coords': v[2],
                        'name': v[5],
                        'value': v[5],
                        'belongs_to': self.datasets[int(v[6])][int(v[4])],
                        'annot_index': v[7]}
            for v in image_map if self.datasets[int(v[6])][int(v[4])]['metric_data'] == 'data_settings' and
                self.datasets[int(v[6])][int(v[4])]['type'] == 'range_annotation')

        for r in data_result:
            if r['belongs_to']['uid'] not in ret_res['data']:
                ret_res['data'][r['belongs_to']['uid']] = {}
            ret_res['data'][r['belongs_to']['uid']][r['index']] = r

        for r in annotation_result:
            coords = map(int, r['coords'].split(','))
            coords[3] = coords[1] + self._ANNOTATION_MARK_HEIGHT
            r['coords'] = ','.join(map(str, coords))
            ret_res['point_annotations'][int(r['annot_index'])] = r

        for r in range_annotation_result:
            coords = map(int, r['coords'].split(','))
            coords[3] = coords[1] + self._ANNOTATION_MARK_HEIGHT
            r['coords'] = ','.join(map(str, coords))
            ret_res['range_annotations'][int(r['annot_index'])] = r
            
        return ret_res

    def draw_range_annotations(self):
        """
        draw collected range annotations
        """

        for range_annotation in self._range_annotations:
            self._draw_range_annotation(range_annotation)

            # add layer for annotation markers
            self.datasets[self.layers_count] = {}
            self.datasets[self.layers_count][0] = {'uid': range_annotation['uid'], 'metric_data': range_annotation['element_type'], 'type': 'range_annotation'}
            self.dataset_ids[self.layers_count] = 0
            self.layers_count += 1

            self.datasets[self.layers_count] = {}
            self.datasets[self.layers_count][0] = {'uid': range_annotation['uid'], 'metric_data': range_annotation['element_type'], 'type': 'left_range_annotation'}
            self.dataset_ids[self.layers_count] = 0
            self.layers_count += 1

            self.datasets[self.layers_count] = {}
            self.datasets[self.layers_count][0] = {'uid': range_annotation['uid'], 'metric_data': range_annotation['element_type'], 'type': 'right_range_annotation'}
            self.dataset_ids[self.layers_count] = 0
            self.layers_count += 1

            self.datasets[self.layers_count] = {}
            self.datasets[self.layers_count][0] = {'uid': range_annotation['uid'], 'metric_data': range_annotation['element_type'], 'type': 'line_range_annotation'}
            self.dataset_ids[self.layers_count] = 0
            self.layers_count += 1
