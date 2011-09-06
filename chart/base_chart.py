# -*- coding: utf-8 -*-

from pychartdir import *
import datetime
import urllib
import re
from base_date_chart import AbstractBaseDateChart
from operator import itemgetter
from pprint import pprint


class BaseChart(AbstractBaseDateChart):
    annotations_map = []
    flipped_bars_annot_coords = [13, 10, 12, -10]
    report_data_set_chart_id = 0
    report_data_set_instance_id = 0

    def __init__(self, data, jfile, settings):
        self.annotations_map = []
        
        AbstractBaseDateChart.__init__(self, data, jfile, settings)
        self._is_x_axis_date = self.settings['is_x_axis_date']
        if self.layout_data['use_stacked_bars_ind'] == 'Y':
            header_len = len(self.chart_data['orig_header'])
            self._stacked_bar_annotation_indexes[0] = [0] * header_len
            self._stacked_bar_annotation_indexes[1] = [0] * header_len
#            for i in self.chart_data['orig_header']:
#                self._stacked_bar_annotation_indexes[0].append(0)
#                self._stacked_bar_annotation_indexes[1].append(0)
        if self._is_x_axis_date:
            self._prepare_axes_scaling(self.chart_data['orig_header'])
        else:
            self._prepare_y_axis_scaling()

    def prepare_data(self):
#        self.layout_data['use_stacked_bars_ind'] = 'N'
#        #change None to NoValue in the data lists
#        self.layout_data['use_stacked_bars_ind'] = 'Y'
#        self.chart_data['settings']['US order Volume (US$)2'] = copy.deepcopy(self.chart_data['settings']['US order Volume (US$)'])
#        self.chart_data['settings']['US order Volume (US$)2']['color'] = 0xff0000
#
#        """
#        self.chart_data['settings']['Order Count2'] = copy.deepcopy(self.chart_data['settings']['Order Count'])
#        self.chart_data['settings']['Order Count2']['color'] = 0x00ff00
#        self.chart_data['range_annotations']['Order Count2'] = []
#        self.chart_data['annotations']['Order Count2'] = copy.deepcopy(self.chart_data['annotations']['Order Count'])
#        """
#
#        self.layout_data['secondary_y_axis_values'] = set(['Intl order Volume (US$)', 'Intl order Volume (US$)2'])
#
#        self.chart_data['range_annotations']['US order Volume (US$)2'] = []
#        self.chart_data['range_annotations']['Intl order Volume (US$)2'] = []
#
#        self.chart_data['annotations']['US order Volume (US$)2'] = []
#        self.chart_data['annotations']['Intl order Volume (US$)2'] = []
        if self.layout_data['flip_x_and_y_axis'] == 'Y':
            self.chart_data['orig_header'].reverse()
            #self.chart_data['formatted_header'].reverse()
            self.chart_data['labels'].reverse()
            self.chart_data['x_scale_labels'].reverse()

        for element, data in self.chart_data['settings'].iteritems():
            if data:
                if self.layout_data['flip_x_and_y_axis'] == 'Y':
                    data['data'].reverse()
                    data['formatted_data'].reverse()

                #data['display_type'] = 'bar'
                #self.chart_data['settings'][element]['display_type'] = 'bar'
                if element in self.layout_data['secondary_y_axis_values']:
                    #or (self.layout_data['pivot_total_axis_number'] and str(self.layout_data['pivot_total_axis_number']) == '2' and element == u'TOTAL' and self.layout_data['chart_include_method'] != 'totals only'):
                    axis_index = 1
                    self._has_y_axis_2 = True
                    self.layout_data['y2_axis_font_color'] = data['color']
                    self.chart_data['settings'][element]['axis_number'] = 2
                else:
                    self.chart_data['settings'][element]['axis_number'] = 1
                    axis_index = 0
                if data['display_type'] == 'bar' and self.layout_data['use_stacked_bars_ind'] == 'Y':
                    if self._stacked_bars_values[axis_index]:
                        self._stacked_bars_values[axis_index] = [v + self._stacked_bars_values[axis_index][i] if v else self._stacked_bars_values[axis_index][i] for i, v in enumerate(data['data'])]
                    else:
                        self._stacked_bars_values[axis_index] = [value if value else 0.0 for value in data['data']]
                else:
                    self._charting_values[axis_index].extend([value for value in data['data'] if value is not None])
                if data['display_type'] == 'line':
                    self._has_lines[axis_index] = True
                    self._line_values[axis_index].append(data['data'])
                elif data['display_type'] == 'bar':
                    self._has_bars[axis_index] += 1
                    self._has_first_bar = True
                    self._has_last_bar = True
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

                                if not range_annotation['right_marker']:
                                    self._has_over_right_border = True
                                if not range_annotation['left_marker']:
                                    self._has_over_left_border = True


                    # check for annotations for first, last points
                    if self.chart_data['point_annotations'][element]:
                        if self.layout_data['flip_x_and_y_axis'] == 'Y':
                            self.chart_data['point_annotations'][element].reverse()
                        if self.chart_data['point_annotations'][element][0]:
                            self._has_first_annotation = True
                        if self.chart_data['point_annotations'][element][-1]:
                            self._has_last_annotation = True
                        #annotations = [data['data'][i] if a else None for i, a in enumerate(self.chart_data['point_annotations'][element])]
                        #self._annotations[axis_index].append(annotations)
                data['data'] = map(self._none_to_no_value, data['data'])

            if self.layout_data['use_stacked_bars_ind'] == 'Y':
                if self._stacked_bars_values[0]:
                    self._charting_values[0].extend([value for value in self._stacked_bars_values[0] if value is not None])
                if self._has_y_axis_2 and self._stacked_bars_values[1]:
                    self._charting_values[1].extend([value for value in self._stacked_bars_values[1] if value is not None])

        if not self._has_bars[0] and not self._has_bars[1]:  
            self.layout_data['use_stacked_bars_ind'] = 'N'

        if self.layout_data['use_stacked_bars_ind'] == 'Y':
            if self._has_bars[0]:
                self._has_bars[0] = 1
            if self._has_y_axis_2:
                if self._has_bars[1]:
                    self._has_bars[1] = 1

        if self.settings['is_x_axis_date']:
            if self.chart_data['orig_header'][0] > self.chart_data['orig_header'][-1]:
                self._has_over_right_border, self._has_over_left_border = self._has_over_left_border, self._has_over_right_border

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

        self._set_y_axes_formats()

        self._create_layers()

        if self.layout_data['flip_x_and_y_axis'] == 'Y':
            self.c.swapXY()

        bars_are_charted = False
        # this is workout of stacked bars with secondary y axis
        if self.layout_data['use_stacked_bars_ind'] == 'Y' and self._has_y_axis_2 and self._has_bars[0] and self._has_bars[1]:
            bars_are_charted = True
            stacked_layers = [[], []]
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
                data_layer = self._create_bar_layer(self.chart_data['settings'][element]['data'], color, self._y_axis(), 0, 0, self._is_x_axis_date, {'type': 'data', 'name': element, 'shape': 'bar'})
                if data_layer is None:
                    raise Exception("Chart %s misconfigured. Column to use for X axis values must contain numeric values." % self.report_data_set_chart_id)

            # this is stacked bars for secondary y axis
            self.bar_layer.addDataGroup("2")
            for element in stacked_layers[1]:
                color = self._get_element_color(self.chart_data['settings'][element]['display_type'], self.chart_data['settings'][element]['line_type'], self.chart_data['settings'][element]['color'])
                data_layer = self._create_bar_layer(self.chart_data['settings'][element]['data'], color, self._y_axis2(), 1, 1, self._is_x_axis_date, {'type': 'data', 'name': element, 'shape': 'bar'})
                if data_layer is None:
                    raise Exception("Chart %s misconfigured. Column to use for X axis values must contain numeric values." % self.report_data_set_chart_id)

        if self.layout_data['use_stacked_bars_ind'] == 'Y' and self.settings['type'] == 'large' and self.settings['is_x_axis_date'] and (self._has_bars[0] or self._has_bars[1]):
            x_axis_data, x_axis_time, scale_real_bar_width = self._calculate_shift_for_bar(0, self.settings['is_x_axis_date'])
            self._stacked_bar_annotations_x_axis_time[0] = x_axis_time
            if self._has_y_axis_2:
                x_axis_data, x_axis_time, scale_real_bar_width = self._calculate_shift_for_bar(1, self.settings['is_x_axis_date'])
                self._stacked_bar_annotations_x_axis_time[1] = x_axis_time

        bar_ind = -1
        for element, data in self.chart_data['settings'].iteritems():
            # set y axis
            if data['axis_number'] == 2:
                axis = self._y_axis2()
                axis_index = 1
            else:
                axis = self._y_axis()
                axis_index = 0

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
                bar_ind += 1
                # bar layer
                if self.layout_data['use_stacked_bars_ind'] == 'Y':
                    bar_count = axis_index
#                    if self._stacked_bars_values[axis_index]:
#                        self._stacked_bars_values[axis_index] = [v + self._stacked_bars_values[axis_index][i] if v else self._stacked_bars_values[axis_index][i] for i, v in enumerate(data['data'])]
#                    else:
#                        self._stacked_bars_values[axis_index] = [value if value else 0.0 for value in data['data']]
                else:
                    bar_count = bar_ind

                if not bars_are_charted:
                    data_layer = self._create_bar_layer(data['data'], color, axis, axis_index, bar_count, self.settings['is_x_axis_date'], {'type': 'data', 'name': element, 'shape': 'bar'})
                    if data_layer is None:
                        raise Exception("Chart %s misconfigured. Column to use for X axis values must contain numeric values." % self.report_data_set_chart_id)

            if self.settings['type'] == 'large':
                if self.settings['is_x_axis_date']:
                    self._collect_range_annotations(
                                        self.chart_data['range_annotations'][element],
                                        axis_index,
                                        {'element': element})
                if self.layout_data['use_stacked_bars_ind'] == 'Y' and data['display_type'] == 'bar' and self.chart_data['point_annotations'][element]:
                    indexes = []
                    values = []
                    for i, index in enumerate(self._stacked_bar_annotation_indexes[axis_index]):
                        if self.chart_data['point_annotations'][element][i]:
                            indexes.append(index)
                            self._stacked_bar_annotation_indexes[axis_index][i] += 1
                        else:
                            indexes.append(0)
                        values.append(0.0)

                    self._stacked_bar_annotations[axis_index].append({'indexes': indexes,
                                                                      'annotations': self.chart_data['point_annotations'][element],
                                                                      'values': values,
                                                                      'element': element,
                                                                      'x_axis_data': self._stacked_bar_annotations_x_axis_time[axis_index]
                                                                      })
                else:
                    # draw point annotations
                    self._draw_point_annotations(
                            self.chart_data['point_annotations'][element],
                            data['data'],
                            axis,
                            axis_index,
                            data['display_type'],
                            bar_ind,
                            self.settings['is_x_axis_date'],
                            {'type': 'annot', 'name': element, 'shape': 'poly'})

            if self.layout_data['include_legend_ind'] == 'Y':
                axis = self._y_axis()
                if data['display_type'] == 'line':
                    l_legend = self.legend_layer.addDataSet([], self.chart_data['settings'][element]['color'], data['label'])
                    l_legend.setUseYAxis(axis)
                    if self.layout_data['show_line_data_points_ind'] == 'Y':
                        l_legend.setDataSymbol(shape, self.layout_data['line_data_point_dot_size'], color, color)
                    else:
                        l_legend.setDataSymbol(CircleShape, self.layout_data['line_width'], color, color)
                elif data['display_type'] == 'bar':
                    l_legend = self.legend_layer.addDataSet([], self.chart_data['settings'][element]['color'], data['label'])
                    l_legend.setLineWidth(0)
                    l_legend.setDataSymbol4(self._cSquareSymbol, 15, color, -1)
                    l_legend.setUseYAxis(axis)

        if self.layout_data['use_stacked_bars_ind'] == 'Y' and self.settings['type'] == 'large' and (self._has_bars[0] or self._has_bars[1]):
            self._calc_stacked_bars_annotations(self.settings['is_x_axis_date'])

        if self.settings['is_x_axis_date']:
            if self.layout_data['use_stacked_bars_ind'] == 'Y' and (self._has_bars[0] or self._has_bars[1]):
                x_axis_data, x_axis_time, scale_real_bar_width = self._calculate_shift_for_bar(0, True)
                self._bar_values[0].append({'x_axis_data': x_axis_time, 'values': self._stacked_bars_values[0], 'width': scale_real_bar_width})

                if self._has_y_axis_2:
                    x_axis_data, x_axis_time, scale_real_bar_width = self._calculate_shift_for_bar(1, True)
                    self._bar_values[1].append({'x_axis_data': x_axis_time, 'values': self._stacked_bars_values[1], 'width': scale_real_bar_width})

            if self.layout_data['include_annotations_ind'] == 'Y':
                if self._range_annotations:
                    self._calc_annotations()
                    self.draw_range_annotations()

                if self.layout_data['use_stacked_bars_ind'] == 'Y' and (self._has_bars[0] or self._has_bars[1]):
                    self.draw_stacked_bars_annotations()
            
            self._set_y_axes_scaling()
        else:
            if self.layout_data['include_annotations_ind'] == 'Y' and self.layout_data['use_stacked_bars_ind'] == 'Y' and (self._has_bars[0] or self._has_bars[1]):
                self.draw_stacked_bars_annotations()

            # remove y-axis rounding labels so top value can be positioned near to top
            if self.settings['type'] == 'thumbnail':
                self._y_axis().setRounding(False, False)
                self._y_axis2().setRounding(False, False)

            self._set_y_axes_scaling()
#            autoscale_from_zero = (0.1, 0.1, 1)
#            autoscale_from_any = (0.1, 0.1, 0.4)
#            if self.layout_data['use_stacked_bars_ind'] == 'Y' and self._has_bars[0] or (self._has_y_axis_2 and self._has_bars[1]):
#                self._set_y_axes_scaling()
#            else:
#                if self._y_axis_autoscale[0]:
#                    self._y_axis().setAutoScale(*autoscale_from_any)
#                else:
#                    self._y_axis().setAutoScale(*autoscale_from_zero)
#                if self._has_y_axis_2:
#                    if self._y_axis_autoscale[1]:
#                        self._y_axis2().setAutoScale(*autoscale_from_any)
#                    else:
#                        self._y_axis2().setAutoScale(*autoscale_from_zero)

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
            if self.settings['is_index']:
                self.create_resized_preview()

    def create_map_file(self):
        map = self.get_parsed_map()
        data = []
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
        ret_res = {
            'data': {},
            'annotations': [],
            'range_annotations': {}
        }

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

        point_annotations = {}
        for r in annotation_result:
            point_annotations[int(r['annot_index'])] = r

        if self.layout_data['flip_x_and_y_axis'] == 'Y':
            for element, data in self.chart_data['settings'].iteritems():
                if data['display_type'] == 'bar':
                    for index, annotation_mark in point_annotations.iteritems():
                        if index in self.chart_data['annotations'][element]:
                            real_coords = map(int, annotation_mark['coords'].split(','))
                            fixed_coords = [real_coords[j] + self.flipped_bars_annot_coords[j] for j in range(4)]
                            point_annotations[index]['coords'] = ','.join(map(str, fixed_coords))

        for index, annotation_mark in point_annotations.iteritems():
            for element in self.chart_data['settings'].iterkeys():
                if index in self.chart_data['annotations'][element]:
                    for point_annotation in self.chart_data['annotations'][element][index]:
                        point_annotation['index'] = index

                        if self.layout_data['flip_x_and_y_axis'] != 'Y':
                            coords = map(int, annotation_mark['coords'].split(','))
                            coords[3] = coords[1] + self._ANNOTATION_MARK_HEIGHT
                            annotation_mark['coords'] = ','.join(map(str, coords))

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

        range_annotations = {}
        for r in range_annotation_result:
            range_annotations[int(r['annot_index'])] = r

        for index, annotation_mark in range_annotations.iteritems():
            for element in self.chart_data['settings'].iterkeys():
                if index in self.chart_data['annotations'][element]:
                    range_annotation = self.chart_data['annotations'][element][index]
                    range_annotation['index'] = index
                    if self.layout_data['flip_x_and_y_axis'] != 'Y':
                        coords = map(int, annotation_mark['coords'].split(','))
                        coords[3] = coords[1] + self._ANNOTATION_MARK_HEIGHT
                        annotation_mark['coords'] = ','.join(map(str, coords))
                    range_annotation['coords'] = annotation_mark['coords']
                    range_annotation['shape'] = annotation_mark['shape']
                    range_annotation['annotation_interval'] = 'range'
                    range_annotation['value'] = ''
                    ret_res['annotations'].append(range_annotation)
        ret_res['annotations'].sort(key=itemgetter('index'))
        return ret_res

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
        else:
            self.c.xAxis().setLabels(self.chart_data['labels'])
            self.c.xAxis().setLabelStep(1)
        self._markup_plot_area()

    def draw_range_annotations(self):
        """
        draw collected range annotations
        """

        # draw range annotations
        for range_annotation in self._range_annotations:
            self._draw_range_annotation(range_annotation)

            # add layer for annotation markers
            self.datasets[self.layers_count] = {}
            self.datasets[self.layers_count][0] = {'type': 'range_annot', 'name': range_annotation['element'], 'shape': 'poly'}
            self.dataset_ids[self.layers_count] = 0
            self.layers_count += 1

            self.datasets[self.layers_count] = {}
            self.datasets[self.layers_count][0] = {'type': 'left_range_annot', 'name': range_annotation['element'], 'shape': 'poly'}
            self.dataset_ids[self.layers_count] = 0
            self.layers_count += 1

            self.datasets[self.layers_count] = {}
            self.datasets[self.layers_count][0] = {'type': 'right_range_annot', 'name': range_annotation['element'], 'shape': 'poly'}
            self.dataset_ids[self.layers_count] = 0
            self.layers_count += 1

            self.datasets[self.layers_count] = {}
            self.datasets[self.layers_count][0] = {'type': 'line_range_annot', 'name': range_annotation['element'], 'shape': 'poly'}
            self.dataset_ids[self.layers_count] = 0
            self.layers_count += 1

    def draw_stacked_bars_annotations(self):
        axes = [0]
        if self._has_y_axis_2:
            axes = [0, 1]
        for axis_index in axes:
            # set y axis
            if axis_index == 1:
                axis = self._y_axis2()
            else:
                axis = self._y_axis()

            for annotations in self._stacked_bar_annotations[axis_index]:
                # draw point annotations
                self._draw_point_annotations(
                        annotations['annotations'],
                        annotations['values'],
                        axis,
                        axis_index,
                        'bar',
                        axis_index,
                        self.settings['is_x_axis_date'],
                        {'type': 'annot', 'name': annotations['element'], 'shape': 'poly'})

