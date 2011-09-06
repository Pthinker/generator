# -*- coding: utf-8 -*-

from fontManager import FontManager
from pychartdir import *
import datetime
import urllib
import re
from baseDateChart import AbstractBaseDateChart
import copy
from pprint import pprint


class BaseChart(AbstractBaseDateChart):
    annotations_map = list()
    flipped_bars_annot_coords = [13, 10, 12, -10]
    report_data_set_chart_id = 0
    report_data_set_instance_id = 0
    stacked_bars = [list(), list()]

    def __init__(self, data, jfile, settings):
        self.stacked_bars = [list(), list()]
        self.annotations_map = list()
        AbstractBaseDateChart.__init__(self, data, jfile, settings)
        if self.settings['is_x_axis_date']:
            self._prepare_axes_scaling(self.chart_data['orig_header'])

    def prepare_data(self):
#        #change None to NoValue in the data lists
#        self.layout_data['use_stacked_bars_ind'] = 'Y'
#        self.chart_data['settings']['US order Volume (US$)2'] = copy.deepcopy(self.chart_data['settings']['US order Volume (US$)'])
#        self.chart_data['settings']['US order Volume (US$)2']['color'] = 0xff0000
#
        """
        self.chart_data['settings']['Order Count2'] = copy.deepcopy(self.chart_data['settings']['Order Count'])
        self.chart_data['settings']['Order Count2']['color'] = 0x00ff00
        self.chart_data['range_annotations']['Order Count2'] = []
        self.chart_data['annotations']['Order Count2'] = copy.deepcopy(self.chart_data['annotations']['Order Count'])
        """
#
#        self.layout_data['secondary_y_axis_values'] = set(['Intl order Volume (US$)', 'Intl order Volume (US$)2'])
#
#        self.chart_data['range_annotations']['US order Volume (US$)2'] = []
#        self.chart_data['range_annotations']['Intl order Volume (US$)2'] = []
#
#        self.chart_data['annotations']['US order Volume (US$)2'] = []
#        self.chart_data['annotations']['Intl order Volume (US$)2'] = []
        for element, data in self.chart_data['settings'].iteritems():
            if data:
                #data['display_type'] = 'bar'
                #self.chart_data['settings'][element]['display_type'] = 'bar'
                if element in self.layout_data['secondary_y_axis_values'] or (self.layout_data['pivot_total_axis_number'] and str(self.layout_data['pivot_total_axis_number']) == '2' and element == 'TOTAL'):
                    axis = 1
                    self._has_y_axis_2 = True
                    self.layout_data['y2_axis_font_color'] = data['color']
                    self.chart_data['settings'][element]['axis_number'] = 2
                else:
                    self.chart_data['settings'][element]['axis_number'] = 1
                    axis = 0

                if data['display_type'] == 'bar' and self.layout_data['use_stacked_bars_ind'] == 'Y':
                    if self.stacked_bars[axis]:
                        self.stacked_bars[axis] = [v + self.stacked_bars[axis][i] if v else self.stacked_bars[axis][i] for i, v in enumerate(data['data'])]
                    else:
                        self.stacked_bars[axis] = [value if value else 0.0 for value in data['data']]
                else:
                    self._charting_values[axis].extend([value for value in data['data'] if value is not None])
                if data['display_type'] == 'line':
                    self._has_lines[axis] = True
                    self._line_values[axis].append(data['data'])
                elif data['display_type'] == 'bar':
                    self._has_bars[axis] += 1
                    self._has_first_bar = True
                    self._has_last_bar = True
#                    if data['data'][0] is not None:
#                        self._has_first_bar = True
#                    if data['data'][-1] is not None:
#                        self._has_last_bar = True
                if self.layout_data['include_annotations_ind'] == 'Y':
                    # process range annotations
                    if self.settings['is_x_axis_date']:
                        if self.chart_data['range_annotations'][element]:
                            for i, range_annotation in enumerate(self.chart_data['range_annotations'][element]):
                                self.chart_data['range_annotations'][element][i]['from_time_value'] = time.mktime(range_annotation['from_time'].timetuple())
                                self.chart_data['range_annotations'][element][i]['to_time_value'] = time.mktime(range_annotation['to_time'].timetuple())
                                self.chart_data['range_annotations'][element][i]['middle_time_value'] = (range_annotation['from_time_value'] + range_annotation['to_time_value']) / 2.0
                                self.chart_data['range_annotations'][element][i]['middle_time'] = datetime.datetime.fromtimestamp(range_annotation['middle_time_value'])
                                self._range_annotations_middle_points.append(self.chart_data['range_annotations'][element][i]['middle_time_value'])

                    # check for annotations for first, last points
                    if self.chart_data['point_annotations'][element]:
                        if self.chart_data['point_annotations'][element][0]:
                            self._has_first_annotation = True
                        if self.chart_data['point_annotations'][element][-1]:
                            self._has_last_annotation = True
                        #annotations = [data['data'][i] if a else None for i, a in enumerate(self.chart_data['point_annotations'][element])]
                        #self._annotations[axis].append(annotations)

            if self.layout_data['use_stacked_bars_ind'] == 'Y':
                if self.stacked_bars[0]:
                    self._charting_values[0].extend([value for value in self.stacked_bars[0] if value is not None])
                if self._has_y_axis_2:
                    if self.stacked_bars[1]:
                        self._charting_values[1].extend([value for value in self.stacked_bars[1] if value is not None])

        if self.layout_data['use_stacked_bars_ind'] == 'Y':
            if self._has_bars[0]:
                self._has_bars[0] = 1
            if self._has_y_axis_2:
                if self._has_bars[1]:
                    self._has_bars[1] = 1

    def set_chart_ids(self, report_data_set_chart_id, report_data_set_instance_id):
        self.report_data_set_chart_id = report_data_set_chart_id
        self.report_data_set_instance_id = report_data_set_instance_id

    def create_chart(self):

        self._create_plot_area()

        if self.settings['is_x_axis_date']:
            self._prepare_x_scale_values()

        self._set_x_axis_props()
        self._set_y_axis_props()

        self.chart_data['even_header'] = range(len(self.chart_data['labels']))

        if self.settings['is_x_axis_date']:
            self._set_x_axis_date_scale()
        else:
            self._set_x_axis_plain_scale()

        if self.layout_data['use_stacked_bars_ind'] == 'Y':
            is_stacked = True
        else:
            is_stacked = False

        self._create_layers(is_stacked)

        if self.layout_data['flip_x_and_y_axis'] == 'Y':
            self.c.swapXY()

        # this is workout of stacked bars with secondary y axis
        if self.layout_data['use_stacked_bars_ind'] == 'Y' and self._has_y_axis_2:
            stacked_layers = [list(), list()]
            for element, data in self.chart_data['settings'].iteritems():
                if data['display_type'] == 'bar':
                    if data['axis_number'] == 2:
                        stacked_layers[1].append(element)
                    else:
                        stacked_layers[0].append(element)
            # this is stacked bars for primary y axis
            self.bar_layer.addDataGroup("1")
            for element in stacked_layers[0]:
                color = self._get_element_color(self.chart_data['settings'][element]['display_type'], self.chart_data['settings'][element]['line_type'], self.chart_data['settings'][element]['color'])
                data_layer = self._create_bar_layer(self.chart_data['settings'][element]['data'], color, self.c.yAxis2(), {'type': 'data', 'name': element, 'shape': 'bar'})
                if data_layer is None:
                    raise Exception("Chart %s misconfigured. Column to use for X axis values must contain numeric values." % self.report_data_set_chart_id)

            # this is stacked bars for secondary y axis
            self.bar_layer.addDataGroup("2")
            for element in stacked_layers[1]:
                color = self._get_element_color(self.chart_data['settings'][element]['display_type'], self.chart_data['settings'][element]['line_type'], self.chart_data['settings'][element]['color'])
                data_layer = self._create_bar_layer(self.chart_data['settings'][element]['data'], color, self.c.yAxis2(), {'type': 'data', 'name': element, 'shape': 'bar'})
                if data_layer is None:
                    raise Exception("Chart %s misconfigured. Column to use for X axis values must contain numeric values." % self.report_data_set_chart_id)

        bar_ind = -1
        for element, data in self.chart_data['settings'].iteritems():

            if data['axis_number'] == 2:
                axis = self.c.yAxis2()
                curr_axis = 1
            else:
                axis = self.c.yAxis()
                curr_axis = 0

            #if not data.has_key('line_type'):
            if 'line_type' not in data:
                data['line_type'] = ''
            color = self._get_element_color(data['display_type'], data['line_type'], data['color'])

            shape = None

            if data['display_type'] == 'line':
                # line layer
                line_description = {'type': 'line_data', 'name': element, 'shape': ''}
                line_point_description = {'type': 'data', 'name': element, 'shape': 'point'}
                if self.layout_data['show_line_data_points_ind'] == 'Y':
                    show_data_points = True
                    shape = self._set_shape(self.chart_data['settings'][element]['line_point_shape'])
                else:
                    show_data_points = False
                    shape = None
                line_width = None
                data_layer = self._create_line_layer(data['data'], data['line_style'], line_width, color, axis, show_data_points, shape, line_description, line_point_description)
                if data_layer is None:
                    raise Exception("Chart %s misconfigured. Column to use for X axis values must contain numeric values." % self.report_data_set_chart_id)
            else:
                # bar layer
                bar_ind += 1
                if not (self.layout_data['use_stacked_bars_ind'] == 'Y' and self._has_y_axis_2):
                    data_layer = self._create_bar_layer(data['data'], color, axis, {'type': 'data', 'name': element, 'shape': 'bar'})
                    if data_layer is None:
                        raise Exception("Chart %s misconfigured. Column to use for X axis values must contain numeric values." % self.report_data_set_chart_id)

            if self.settings['type'] == 'large':
                if self.settings['is_x_axis_date']:
                    self._collect_range_annotations(
                                        self.chart_data['range_annotations'][element],
                                        curr_axis,
                                        {'element': element})
                
                # draw point annotations
                self._draw_point_annotations(
                        self.chart_data['point_annotations'][element],
                        data['data'],
                        axis,
                        curr_axis,
                        data['display_type'],
                        bar_ind,
                        self.settings['is_x_axis_date'],
                        {'type': 'annot', 'name': element, 'shape': 'poly'})

            if self.layout_data['include_legend_ind'] == 'Y':
                axis = self.c.yAxis()
                if data['display_type'] == 'line':
                    l_legend = self.legend_layer.addDataSet(list(), self.chart_data['settings'][element]['color'], data['label'])
                    l_legend.setUseYAxis(axis)
                    if self.layout_data['show_line_data_points_ind'] == 'Y':
                        l_legend.setDataSymbol(shape, self.layout_data['line_data_point_dot_size'], color, color)
                    else:
                        l_legend.setDataSymbol(CircleShape, self.layout_data['line_width'], color, color)
                elif data['display_type'] == 'bar':
                    l_legend = self.legend_layer.addDataSet(list(), self.chart_data['settings'][element]['color'], data['label'])
                    l_legend.setLineWidth(0)
                    l_legend.setDataSymbol4(self._cSquareSymbol, 15, color, -1)
                    l_legend.setUseYAxis(axis)

        if self.settings['is_x_axis_date']:
            if self.layout_data['include_annotations_ind'] == 'Y':
                if self._range_annotations:
                    self._calc_annotations()
                    self.draw_range_annotations()
            self._set_y_axes_scaling()
        else:
            # remove y-axis rounding labels so top value can be positioned near to top
            if self.settings['type'] == 'thumbnail':
                self.c.yAxis().setRounding(False, False)
                self.c.yAxis2().setRounding(False, False)

            autoscale_from_zero = (0.1, 0.1, 1)
            autoscale_from_any = (0.1, 0.1, 0.4)

            if self._y_axis_autoscale[0]:
                self.c.yAxis().setAutoScale(*autoscale_from_any)
            else:
                self.c.yAxis().setAutoScale(*autoscale_from_zero)
            if self._has_y_axis_2:
                if self._y_axis_autoscale[1]:
                    self.c.yAxis2().setAutoScale(*autoscale_from_any)
                else:
                    self.c.yAxis2().setAutoScale(*autoscale_from_zero)

        self._pack_plot_area()

        filename = ''
        if self.settings['type'] == 'large':
            filename = self._jfile.get_chart_file_name(self.report_data_set_chart_id, self.report_data_set_instance_id)
        elif self.settings['type'] == 'thumbnail':
            filename = self._jfile.get_thumbnail_file_name()
        elif self.settings['type'] == 'preview':
            filename = self._jfile.get_preview_file_name()

        # draw chart
        if not self.c.makeChart(filename):
            raise Exception("ChartDirector cannot create image file %s for chart %s." % (filename, self.report_data_set_chart_id))

        self.file_man.change_perm(filename)
        if self.settings['type'] == 'large':
            self.create_map_file()

    def create_map_file(self):
        map = self.get_parsed_map()
        data = list()
        for v in map['data']:
            label = self.chart_data['labels'][v['label_index']]
            v['value'] = self.chart_data['settings'][v['name']]['formatted_data'][v['label_index']]
            orig_header = ''
            if self.settings['is_x_axis_date']:
                orig_header = str(self.chart_data['orig_header'][v['label_index']])
            data.append({
                         'shape': v['shape'],
                         'coords': v['coords'],
                         'name': self.chart_data['settings'][v['name']]['label'],
                         'meas_index': label,
                         'raw_meas_index': orig_header,
                         'value': v['value']})

        self.annotations_map = map['annotations']
        self._jfile.make_chart_file(data, map['annotations'], self.report_data_set_chart_id, self.report_data_set_instance_id, 'line-bar', self.settings['is_x_axis_date'])

    def get_parsed_map(self):
        ret_res = dict()
        ret_res['data'] = dict()
        ret_res['annotations'] = list()
        ret_res['range_annotations'] = dict()

        content = self.c.getHTMLImageMap(
                'url',
                'x={x}&dataSet={dataSet}&dataSetName={dataSetName}&value={value}&layerId={layerId}&extra_field={field0}')
        content = urllib.unquote(content)
        res = re.findall(r'(<area shape="(.+?)" coords="(.+?)" href="url\?x=(.+?)&dataSet=(.+?)&dataSetName=&value=(.+?)&layerId=(.+?)&extra_field=(.*?)">)', content)

        ret_res['data'] = list({'shape': v[1],
                                'coords': v[2],
                                'name': self.datasets[int(v[6])][int(v[4])]['name'],
                                'value': v[6],
                                'label_index': int(v[7]),
                                'draw_shape_order': 0 if self.datasets[int(v[6])][int(v[4])]['shape'] == 'point' else 1}
                              for v in res if self.datasets[int(v[6])][int(v[4])]['type'] == 'data')

        # resulted points must be sorted by following rule: 1. points (lines) then 2. bars. otherwise bars are overlapping points coordinates
        ret_res['data'] = sorted(ret_res['data'], key=lambda k: k['draw_shape_order'])

        annotation_result = list({'shape': v[1],
                                'coords': v[2],
                                'name': self.datasets[int(v[6])][int(v[4])]['name'],
                                'value': v[6],
                                #'label_index': int(v[7])
                                'annot_index': int(v[7])}
                            for v in res if self.datasets[int(v[6])][int(v[4])]['type'] == 'annot')

        point_annotations = dict()
        for r in annotation_result:
            point_annotations[int(r['annot_index'])] = r

        if self.layout_data['flip_x_and_y_axis'] == 'Y':
            for element, data in self.chart_data['settings'].iteritems():
                if data['display_type'] == 'bar':
                    for index, annotation_mark in point_annotations.iteritems():
                        #if self.chart_data['annotations'][element].has_key(index):
                        if index in self.chart_data['annotations'][element]:
                            real_coors = map(int, annotation_mark['coords'].split(','))
                            fixed_coords = [real_coors[j] + self.flipped_bars_annot_coords[j] for j in range(4)]
                            point_annotations[index]['coords'] = ','.join(map(str, fixed_coords))

        for index, annotation_mark in point_annotations.iteritems():
            for element in self.chart_data['settings'].iterkeys():
                #if self.chart_data['annotations'][element].has_key(index):
                if index in self.chart_data['annotations'][element]:
                    for point_annotation in self.chart_data['annotations'][element][index]:
                        point_annotation['index'] = index
                        point_annotation['coords'] = annotation_mark['coords']
                        point_annotation['shape'] = annotation_mark['shape']
                        point_annotation['annotation_interval'] = 'point'
                        point_annotation['start_time'] = ''
                        point_annotation['finish_time'] = ''
                        point_annotation['raw_start_time'] = ''
                        point_annotation['raw_finish_time'] = ''
                        point_annotation['value'] = point_annotation['formatted_value']
                        ret_res['annotations'].append(point_annotation)

        range_annotation_result = list({'shape': v[1],
                                        'coords': v[2],
                                        'name': self.datasets[int(v[6])][int(v[4])]['name'],
                                        'value': v[6],
                                        'annot_index': v[7]}
                                      for v in res if self.datasets[int(v[6])][int(v[4])]['type'] == 'range_annot')

        range_annotations = dict()
        for r in range_annotation_result:
            range_annotations[int(r['annot_index'])] = r

        for index, annotation_mark in range_annotations.iteritems():
            for element in self.chart_data['settings'].iterkeys():
                #if self.chart_data['annotations'][element].has_key(index):
                if index in self.chart_data['annotations'][element]:
                    range_annotation = self.chart_data['annotations'][element][index]
                    range_annotation['index'] = index
                    range_annotation['coords'] = annotation_mark['coords']
                    range_annotation['shape'] = annotation_mark['shape']
                    range_annotation['annotation_interval'] = 'range'
                    range_annotation['value'] = ''
                    ret_res['annotations'].append(range_annotation)

        return ret_res
#        return data_result

#    def unescape(self, s):
#        p = htmllib.HTMLParser(None)
#        p.save_bgn()
#        p.feed(s)
#        return p.save_end()

    def _set_x_axis_plain_scale(self):
        """
        set x axis data for non-date chart
        """
        len_labels = len(self.chart_data['labels'])
        self._x_axis_data = range(len_labels)
        if self.layout_data['include_x_axis_label_ind'] == 'Y':
            self.c.xAxis().setLabels(self.chart_data['labels'])
            if len_labels > self.layout_data['max_x_axis_labels']:
                label_step, remainder = divmod(len_labels, self.layout_data['max_x_axis_labels'])
                if remainder:
                    label_step += 1
            else:
                label_step = 1
            self.c.xAxis().setLabelStep(label_step)

    def draw_range_annotations(self):
        """
        draw collected range annotations
        """

        # draw range annotations
        for range_annotation in self._range_annotations:
            self._draw_range_annotation(range_annotation)

            # add layer for annotation markers
            self.datasets[self.layers_count] = dict()
            self.datasets[self.layers_count][0] = {'type': 'range_annot', 'name': range_annotation['element'], 'shape': 'poly'}
            self.dataset_ids[self.layers_count] = 0
            self.layers_count += 1

            self.datasets[self.layers_count] = dict()
            self.datasets[self.layers_count][0] = {'type': 'left_range_annot', 'name': range_annotation['element'], 'shape': 'poly'}
            self.dataset_ids[self.layers_count] = 0
            self.layers_count += 1

            self.datasets[self.layers_count] = dict()
            self.datasets[self.layers_count][0] = {'type': 'right_range_annot', 'name': range_annotation['element'], 'shape': 'poly'}
            self.dataset_ids[self.layers_count] = 0
            self.layers_count += 1

            self.datasets[self.layers_count] = dict()
            self.datasets[self.layers_count][0] = {'type': 'line_range_annot', 'name': range_annotation['element'], 'shape': 'poly'}
            self.dataset_ids[self.layers_count] = 0
            self.layers_count += 1
