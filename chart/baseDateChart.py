#-*- coding: utf-8 -*-

from metric.conf import ConfigReader
from pychartdir import *
from metric.file_man.jfiles import JChartFile
from metric.formatter import FieldFormatter
from fontManager import FontManager
from heapq import nlargest
import time
import math
import pprint


class AbstractBaseDateChart:
    # annotation mark full height
    _ANNOTATION_MARK_HEIGHT = 24

    # data point radius
    _DATA_POINT_SIZE = 2

    # half size of range annotation line height, including border markers
    _RANGE_ANNOTATION_LINE_HEIGHT_HALF = 8

    # half size of annotation mark width
    _ANNOTATION_MARK_WIDTH_HALF = 10

    # margin between highest/lowest point and chart border
    _SCALE_MARGIN = 10.0
    _PREVIEW_SCALE_MARGIN = 5.0

    # ids of standard layers
    LINE_LAYER_IND = 0
    SPLINE_LAYER_IND = 1
    BAR_LAYER_IND = 2
    LEGEND_LAYER_IND = 3

    # margin between any charting element and image left/right border
    _plot_area_width_margin = 15

    _y_axis_autoscale = [True, True]

    _ticks = [0, 0]

    _y_axis_range = [None, None]
    _y_axis2_range = [None, None]

    _has_y_axis_2 = False

    _real_y_max_value = [None, None]
    _y_max_value = [None, None]

    _real_y_min_value = [None, None]
    _y_min_value = [None, None]

    _x_max_value = None
    _x_min_value = None

    _charting_values = [list(), list()]
    _line_values = [list(), list()]
    _cSquareSymbol = [-500, 0, 500, 0, 500, 1000, -500, 1000]
#                                cWhiskerSymbol = [
#                                                  -500, 1000,
#                                                  500, 1000,
#                                                  500, 980,
#                                                  10, 980,
#                                                  10, 20,
#                                                  500, 20,
#                                                  500, 0,
#                                                  -500, 0,
#                                                  -500, 20,
#                                                  -10, 20,
#                                                  -10, 980,
#                                                  -500, 980
#                                ]

    _x_axis_time_values = []

    _has_lines = [False, False]
    _has_bars = [0, 0]
    _range_annotations = []
    _range_annotations_middle_points = []
    _has_first_annotation = False
    _has_last_annotation = False
    _annotations = []

    _real_chart_area_width = 0
    _real_chart_area_height = 0
    _has_first_bar = False
    _has_last_bar = False
    _bars_in_group_count = 0

    x_axis_prop = dict()
    y_axis_prop = dict()

    _x_axis_data = []

    _x_axis_labels = []
    _x_axis_values = []

    left_margin = 0.0
    right_margin = 0.0

    layout_data = None
    settings = None
    chart_data = None

    formatter = None

    c = None
    legendbox = None
    legend_font = None
    legend_font_size = None

    datasets = dict()
    dataset_ids = dict()
    layers_count = 0

    line_layer = None
    spline_layer = None
    bar_layer = None
    legend_layer = None

    def __init__(self, data, jfile, settings):
        self.formatter = FieldFormatter(0)

        self.chart_data = data['chart_data']
        self.layout_data = data['layout_data']
        self.settings = settings
        self.file_man = JChartFile()

        self._y_axis_autoscale = [True, True]

        self._y_axis_range = [None, None]
        self._y_axis2_range = [None, None]
        self._has_y_axis_2 = False
        self._real_y_max_value = [None, None]
        self._y_max_value = [None, None]

        self._real_y_min_value = [None, None]
        self._y_min_value = [None, None]

        self._charting_values = [list(), list()]
        self._line_values = [list(), list()]

        self._has_lines = [False, False]
        self._has_bars = [0, 0]
        self._range_annotations = list()
        self._ticks = [0, 0]

        self._real_chart_area_width = 0
        self._real_chart_area_height = 0
        self._has_first_bar = False
        self._has_last_bar = False
        self._bars_in_group_count = 0

        self._x_max_value = None
        self._x_min_value = None
        self._x_axis_time_values = []

        self._has_first_annotation = False
        self._has_last_annotation = False

        self._range_annotations_middle_points = []
        self._annotations = [list(), list()]

        self._x_axis_data = []
        self._x_axis_labels = []
        self._x_axis_values = []

        self.left_margin = 0.0
        self.right_margin = 0.0

        self.datasets = dict()
        self.dataset_ids = dict()
        self.layers_count = 0

        self._jfile = jfile

        self.config = ConfigReader()

        self.prepare_data()
        self._bars_in_group_count = self._has_bars[0]
        if self._has_y_axis_2:
            self._bars_in_group_count += self._has_bars[1]

    def _prepare_axes_scaling(self, header):
        """
        set auto scaling, min/max values for axes
        """

        self._set_y_axis_autoscale(0)
        self._set_real_y_max_value(0)
        self._set_real_y_min_value(0)

        self._x_axis_time_values = [time.mktime(h.timetuple()) for h in header]
        self._x_max_value = self._x_axis_time_values[-1]
        self._x_min_value = self._x_axis_time_values[0]

        if self._has_y_axis_2:
            self._set_y_axis_autoscale(1)
            self._set_y_axis_autoscale(1)
            self._set_real_y_max_value(1)
            self._set_real_y_min_value(1)

        if self.layout_data['show_line_data_points_ind'] == 'Y':
            self._DATA_POINT_SIZE = int(self.layout_data['line_data_point_dot_size'] / 2.0 + 2)

    def prepare_data(self):
        """
        abstract method
        """
        pass

    def _set_axis_properties(self, axis, axis_prop, axis_title):
        """
        set xy axis properties
        """
        axis().setColors(axis_prop['label_font_color'])
        if axis_title:
            axis().setTitle(axis_title, axis_prop['label_font'], axis_prop['label_font_size'], axis_prop['label_font_color'])
        axis().setLabelStyle(axis_prop['label_font'], axis_prop['label_font_size'], axis_prop['label_font_color'], axis_prop['label_font_angle'])

    def _set_y_axis_format(self, y_axis, axis_format):
        """
        set y axis labels format
        """
        if axis_format['show_amount_as'] == 'Thousands':
            divider = '/1000'
        elif axis_format['show_amount_as'] == 'Millions':
            divider = '/1000000'
        else:
            divider = ''
        y_axis().setLabelFormat("%s{={value}%s|%s%s%s}%s" % (
                                                                axis_format['prefix'],
                                                                divider,
                                                                axis_format['display_precision_digits'],
                                                                axis_format['thousands_delimiter'],
                                                                axis_format['decimal_point_delimiter'],
                                                                axis_format['suffix']))
        if not axis_format['display_precision_digits']:
            y_axis().setMinTickInc(1)

    def _set_y_axis_autoscale(self, axis):
        """
        check and set auto scaling if it is enabled
        """
        if self._has_lines[axis] and not self._has_bars[axis]:
            # y-axis has only lines
            if self.layout_data['start_y_axis_from_zero_ind'] == 'Y':
                # axis should include zero point
                self._y_axis_autoscale[axis] = False

        elif self._has_bars[axis]:
            # axis should include zero point
            self._y_axis_autoscale[axis] = False

    def _set_scale_max_min_values(self, axis):
        """
        set min and max values for y axis. it is visible part of scale
        """
        # margin for top/bottom
        if self.settings['type'] == 'preview':
            _SCALE_MARGIN = self._PREVIEW_SCALE_MARGIN
        else:
            _SCALE_MARGIN = self._SCALE_MARGIN
        margin = (self._real_y_max_value[axis] - self._real_y_min_value[axis]) / _SCALE_MARGIN

        self._y_max_value[axis] = self._real_y_max_value[axis] + margin

        if self._y_axis_autoscale[axis]:
            if self._real_y_min_value[axis] > 0.0:
                self._y_min_value[axis] = self._real_y_min_value[axis] - margin
                if self._y_min_value[axis] < 0.0:
                    self._y_min_value[axis] = 0.0
            else:
                self._y_min_value[axis] = self._real_y_min_value[axis] - margin
                if self._real_y_min_value[axis] >= 0.0 and self._y_min_value[axis] < 0.0:
                    self._y_min_value[axis] = 0.0
        else:
            if self._real_y_min_value[axis] < 0.0:
                self._y_min_value[axis] = self._real_y_min_value[axis] - margin
            else:
                self._y_min_value[axis] = 0.0

        if self.layout_data['include_y_axis_label_ind'] == 'Y':
            y_axis_label_font = FontManager.get_db_font(self.layout_data['y_axis_label_font'])
            y_axis_label_font_size = self.layout_data['y_axis_font_size']
            y_axis_label_font_angle = 0

            y_axis_label_height = self.c.getDrawArea().text4('$MK.,0123456789', y_axis_label_font, 0, y_axis_label_font_size, y_axis_label_font_size + 1, y_axis_label_font_angle, False).getHeight()

            num = math.floor(self._real_chart_area_height / (y_axis_label_height + 2))
            step = (self._y_max_value[axis] - self._y_min_value[axis]) / num

            axis_format = None
            if axis == 0 and self.layout_data['primary_y_axis_display_mask']:
                axis_format = self.layout_data['primary_y_axis_display_mask']
            elif axis == 1 and self.layout_data['secondary_y_axis_display_mask']:
                axis_format = self.layout_data['secondary_y_axis_display_mask']

            if axis_format:
                if axis_format['show_amount_as'] == 'Thousands':
                    divider = 1000.0
                elif axis_format['show_amount_as'] == 'Millions':
                    divider = 1000000.0
                else:
                    divider = 1.0
                if axis_format['display_precision_digits']:
                    mul = 10.0 ** axis_format['display_precision_digits']
                else:
                    mul = 1.0
            else:
                mul = 1
                if step < 1:
                    while 10 ** mul * step < 1:
                        mul += 1
                mul *= 1.0
                divider = 1.0
            full_step = int(math.ceil(step * mul / divider))
            if not full_step:
                full_step = 1
            d = 10
            while d < full_step:
                d *= 10

            dividers = [d, d // 2, d // 5]
            real_step = min([d for d in dividers if d >= full_step])

            self._ticks[axis] = real_step * divider / mul

            self._y_max_value[axis] = math.ceil(self._y_max_value[axis] / self._ticks[axis]) * (self._ticks[axis])

            if self._y_axis_autoscale[axis]:
                self._y_min_value[axis] = math.floor(self._y_min_value[axis] / self._ticks[axis]) * (self._ticks[axis])

    def _set_real_y_max_value(self, axis):
        """
        set real max value
        """
        if self._charting_values[axis]:
            self._real_y_max_value[axis] = max(self._charting_values[axis])
        else:
            self._real_y_max_value[axis] = 0.0

    def _set_real_y_min_value(self, axis):
        """
        calculate real min value
        """
        if self._y_axis_autoscale[axis]:
            if self._charting_values[axis]:
                self._real_y_min_value[axis] = min(self._charting_values[axis])
            else:
                self._real_y_min_value[axis] = 0.0
        else:
            self._real_y_min_value[axis] = 0.0

    def _calculate_real_plot_width(self):
        """
        calculate charting plot height
        """

        y_axis_width = 0
        y2_axis_width = 0
        right_margin = left_margin = self._plot_area_width_margin
        # calculate width of left/right y axes and titles
        if self.layout_data['include_y_axis_label_ind'] == 'Y':
            # get width of primary y axis

            # get longest label
            if self.layout_data['primary_y_axis_display_mask']:
                largest_y_label = self.formatter.format_full(self._y_max_value[0], self.layout_data['primary_y_axis_display_mask']['display_mask_id'])
            else:
                largest_y_label = repr(self._y_max_value[0])
            y_axis_width += self.c.getDrawArea().text4(largest_y_label, self.y_axis_prop['label_font'], 0, self.y_axis_prop['label_font_size'], self.y_axis_prop['label_font_size'], self.y_axis_prop['label_font_angle'], False).getWidth()
            y_axis_width += self.c.getDrawArea().text4(self.layout_data['y_axis_title_left'], self.y_axis_prop['label_font'], 0, self.y_axis_prop['label_font_size'], self.y_axis_prop['label_font_size'], 90, False).getWidth()
            y_axis_width += 10

            # get width of secondary y axis
            if self._has_y_axis_2:
                # get longest label
                if self.layout_data['secondary_y_axis_display_mask']:
                    largest_y_label = self.formatter.format_full(self._y_max_value[1], self.layout_data['secondary_y_axis_display_mask']['display_mask_id'])
                else:
                    largest_y_label = repr(self._y_max_value[0])
                y2_axis_width += self.c.getDrawArea().text4(largest_y_label, self.y_axis_prop['label_font'], 0, self.y_axis_prop['label_font_size'], self.y_axis_prop['label_font_size'], self.y_axis_prop['label_font_angle'], False).getWidth()
                y2_axis_width += self.c.getDrawArea().text4(self.layout_data['y_axis_title_right'], self.y_axis_prop['label_font'], 0, self.y_axis_prop['label_font_size'], self.y_axis_prop['label_font_size'], 90, False).getWidth()
                y2_axis_width += 10

        # real chart area width
        self._real_chart_area_width = self.layout_data['chart_object_size_x'] - y_axis_width - y2_axis_width - left_margin - right_margin

        # calculate x-axis margins for bar if needed
        left_bar_margin, right_bar_margin = self._margins_for_bars(len(self.chart_data['orig_header']))

        right_annotation_margin = left_annotation_margin = 0
        if self._has_first_annotation:
            left_annotation_margin = self._ANNOTATION_MARK_WIDTH_HALF + 3
        else:
            if self._range_annotations_middle_points:
                left_range_annotation = min(self._range_annotations_middle_points)
                left_range_annotation_pixels = self._scale_x_value_to_pixels(left_range_annotation)
                if left_range_annotation_pixels < self._ANNOTATION_MARK_WIDTH_HALF + 3:
                    left_annotation_margin = self._ANNOTATION_MARK_WIDTH_HALF + 3

        if self._has_first_annotation:
            right_annotation_margin = self._ANNOTATION_MARK_WIDTH_HALF + 3
        else:
            if self._range_annotations_middle_points:
                right_range_annotation = max(self._range_annotations_middle_points)
                right_range_annotation_pixels = self._scale_x_value_to_pixels(right_range_annotation)
                if right_range_annotation_pixels > self._real_chart_area_width - self._ANNOTATION_MARK_WIDTH_HALF - 3:
                    right_annotation_margin = self._ANNOTATION_MARK_WIDTH_HALF + 3

        self.right_margin = max([right_bar_margin, right_annotation_margin])
        self.left_margin = max([left_bar_margin, left_annotation_margin])

        # reduce real chart area width with margins
        self._real_chart_area_width -= self.right_margin + self.left_margin
        # set x-axis margins for bar if needed
        self._set_x_axis_margin(self.right_margin, self.left_margin)

    def _calculate_real_plot_height(self):
        """
        calculate charting plot height
        """
        # legend height
        legend_height = 0
        if self.layout_data['include_legend_ind'] == 'Y' and self.settings['type'] != 'thumbnail':
            if self.layout_data['include_legend_ind'] == 'Y':
                legendbox_height = 50
                legend_height = self.layout_data['legend_y_coord'] + legendbox_height

        #calculate x axis labels and x axis title height
        x_axis_height = 0
        if self.layout_data['include_x_axis_label_ind'] == 'Y':
            # get longest label
            largest_x_label = nlargest(1, self._x_axis_labels, key=lambda k: len(k))[0]

            x_axis_height = self.c.getDrawArea().text4(largest_x_label, self.x_axis_prop['label_font'], 0, self.x_axis_prop['label_font_size'], self.x_axis_prop['label_font_size'], self.x_axis_prop['label_font_angle'], False).getHeight()
            x_axis_height += 10

            # get x axis title height
            if self.layout_data['x_axis_title']:
                x_axis_height += self.c.getDrawArea().text4(self.layout_data['x_axis_title'], self.x_axis_prop['label_font'], 0, self.x_axis_prop['label_font_size'], self.x_axis_prop['label_font_size'], 0, False).getHeight()

        title_height = 0
        if self.layout_data['include_title_ind'] == 'Y':
            title_height += self.c.getDrawArea().text4(self.layout_data['name'], FontManager.get_db_font(self.layout_data['title_font']), 0, self.layout_data['title_font_size'], self.layout_data['title_font_size'], 0, False).getHeight()
        # calculate real chart plot height
        self._real_chart_area_height = self.layout_data['chart_object_size_y'] - legend_height - x_axis_height - 15 - title_height

    def _scale_pixels_to_y_value(self, pixels, axis):
        """
        scale height in pixels to value. if pure is True, calculate height of some piece of line, else calculate height starting from zero
        """
        return (self._y_max_value[axis] - self._y_min_value[axis]) / self._real_chart_area_height * pixels + self._y_min_value[axis]

    def _scale_y_value_to_pixels(self, value, axis):
        """
        scale some value to pixel measurement
        """
        return int(round(self._real_chart_area_height * (value - self._y_min_value[axis]) / (self._y_max_value[axis] - self._y_min_value[axis])))

    def _scale_x_value_to_pixels(self, value):
        """
        scale some value to pixel measurement
        """
        return int(round(self._real_chart_area_width * (value - self._x_min_value) / (self._x_max_value - self._x_min_value)))

    def _recalculate_real_max_y_value(self, axis, y_max_value):
        """
        recalculate real max y value from max y value
        """
        return self._SCALE_MARGIN / (self._SCALE_MARGIN + 1) * (y_max_value + self._real_y_min_value[axis] / self._SCALE_MARGIN)

    def _calc_annotation_value(self, range_annotation, ind):
        """
        calculate available value for range annotation
        """
        calculated = False
        value_px = self._scale_y_value_to_pixels(range_annotation['value'], range_annotation['axis'])

        from_time_value_px = self._scale_x_value_to_pixels(range_annotation['from_time_value'])
        to_time_value_px = self._scale_x_value_to_pixels(range_annotation['to_time_value'])
        middle_time_value_px = self._scale_x_value_to_pixels(range_annotation['middle_time_value'])

        axes = [0]
        if self._has_y_axis_2:
            axes = [0, 1]

        while not calculated:

            calculated = True
            # check overlapping with all previously calculated range metrics
            for i in xrange(0, ind):
                # check if annotations have overlapping x axis value
                annot_middle_time_value_px = self._scale_x_value_to_pixels(self._range_annotations[i]['middle_time_value'])
                if (range_annotation['from_time'] <= self._range_annotations[i]['to_time'] and range_annotation['to_time'] >= self._range_annotations[i]['from_time']) or \
                        (abs(annot_middle_time_value_px - middle_time_value_px) < self._ANNOTATION_MARK_WIDTH_HALF * 2 + 1):
                    # position of annotation in pixels
                    pixels = self._scale_y_value_to_pixels(self._range_annotations[i]['value'], self._range_annotations[i]['axis'])
                    if value_px > pixels:
                        # if current annotation is higher
                        if (annot_middle_time_value_px + self._ANNOTATION_MARK_WIDTH_HALF < from_time_value_px or \
                                annot_middle_time_value_px - self._ANNOTATION_MARK_WIDTH_HALF > to_time_value_px) and \
                                abs(annot_middle_time_value_px - middle_time_value_px) >= self._ANNOTATION_MARK_WIDTH_HALF * 2 + 1:
                            # if current annotation line does not cross annotation marker
                            diff = self._RANGE_ANNOTATION_LINE_HEIGHT_HALF * 2
                        else:
                            # if current annotation line crosses annotation marker
                            diff = self._ANNOTATION_MARK_HEIGHT + self._RANGE_ANNOTATION_LINE_HEIGHT_HALF
                    else:
                        # if current annotation is lower
                        if (middle_time_value_px + self._ANNOTATION_MARK_WIDTH_HALF < self._scale_x_value_to_pixels(self._range_annotations[i]['from_time_value']) or \
                                middle_time_value_px - self._ANNOTATION_MARK_WIDTH_HALF > self._scale_x_value_to_pixels(self._range_annotations[i]['to_time_value'])) and \
                                abs(annot_middle_time_value_px - middle_time_value_px) >= self._ANNOTATION_MARK_WIDTH_HALF * 2 + 1:
                            # if annotation line does not cross current annotation marker
                            diff = self._RANGE_ANNOTATION_LINE_HEIGHT_HALF * 2
                        else:
                            # if annotation line crosses current annotation marker
                            diff = self._ANNOTATION_MARK_HEIGHT + self._RANGE_ANNOTATION_LINE_HEIGHT_HALF

                    # check if distance of overlapping annotations is less then minimal distance
                    if abs(value_px - pixels) < diff:
                        if (annot_middle_time_value_px + self._ANNOTATION_MARK_WIDTH_HALF < from_time_value_px or \
                                annot_middle_time_value_px - self._ANNOTATION_MARK_WIDTH_HALF > to_time_value_px) and \
                                abs(annot_middle_time_value_px - middle_time_value_px) >= self._ANNOTATION_MARK_WIDTH_HALF * 2 + 1:
                            # if current annotation line does not cross annotation marker
                            diff = self._RANGE_ANNOTATION_LINE_HEIGHT_HALF * 2 + 2
                        else:
                            # if current annotation line crosses annotation marker
                            diff = self._ANNOTATION_MARK_HEIGHT + self._RANGE_ANNOTATION_LINE_HEIGHT_HALF + 2
                        # move current annotation above overlapping annotation to minimal distance
                        value_px = pixels + diff
                        # new value is calculated. check again
                        calculated = False
                        break
                if not calculated:
                    break
            if not calculated:
                continue

            # check overlapping with line points
            for i, x_date in enumerate(self._x_axis_time_values):
                x_date_value = self._scale_x_value_to_pixels(x_date)
                # check if annotation overlap data point
                if (from_time_value_px - self._DATA_POINT_SIZE <= x_date_value and \
                        to_time_value_px + self._DATA_POINT_SIZE >= x_date_value) or \
                        (abs(x_date_value - middle_time_value_px) < self._ANNOTATION_MARK_WIDTH_HALF + self._DATA_POINT_SIZE + 1):
                    for axis in axes:
                        for line_value in self._line_values[axis]:
                            if line_value[i] is not None:
                                # position of line point in pixels
                                pixels = self._scale_y_value_to_pixels(line_value[i], axis)
                                if value_px > pixels:
                                    # annotation is higher than point
                                    diff = self._DATA_POINT_SIZE + self._RANGE_ANNOTATION_LINE_HEIGHT_HALF
                                else:
                                    # annotation is lower than point
                                    if (middle_time_value_px - self._ANNOTATION_MARK_WIDTH_HALF > x_date_value + self._DATA_POINT_SIZE or \
                                            middle_time_value_px + self._ANNOTATION_MARK_WIDTH_HALF < x_date_value - self._DATA_POINT_SIZE) and \
                                            (abs(x_date_value - middle_time_value_px) >= self._ANNOTATION_MARK_WIDTH_HALF + self._DATA_POINT_SIZE + 1):
                                        diff = self._RANGE_ANNOTATION_LINE_HEIGHT_HALF + self._DATA_POINT_SIZE
                                        # annotation marker does not overlap data point
                                    else:
                                        # annotation marker overlaps data point
                                        diff = self._ANNOTATION_MARK_HEIGHT + self._DATA_POINT_SIZE

                                if abs(value_px - pixels) < diff:
                                    # move current annotation above overlapping annotation to minimal distance
                                    value_px = pixels + self._DATA_POINT_SIZE + self._RANGE_ANNOTATION_LINE_HEIGHT_HALF + 2
                                    # new value is calculated. check again
                                    calculated = False
                                    break
                            if not calculated:
                                break
                        if not calculated:
                            break
                    if not calculated:
                        break

            # check if annotation overlap point annotation
            for axis in axes:
                for annotation_set in self._annotations[axis]:
                    point_annotations = annotation_set['values']

                    for i, x_date in enumerate(annotation_set['x_axis_data']):
                        x_date_value = self._scale_x_value_to_pixels(x_date)
                        # check if annotation overlap point annotation
                        if (from_time_value_px - self._ANNOTATION_MARK_WIDTH_HALF <= x_date_value and \
                                to_time_value_px + self._ANNOTATION_MARK_WIDTH_HALF >= x_date_value) or \
                                (abs(x_date_value - middle_time_value_px) < self._ANNOTATION_MARK_WIDTH_HALF * 2 + 1):
                            if point_annotations[i] is not None:
                                # position of point annotation in pixels
                                pixels = self._scale_y_value_to_pixels(point_annotations[i], axis)
                                if value_px > pixels:
                                    # annotation is higher than point annotation
                                    diff = self._RANGE_ANNOTATION_LINE_HEIGHT_HALF + self._ANNOTATION_MARK_HEIGHT
                                else:
                                    # annotation is lower than point annotation
                                    if (middle_time_value_px - self._ANNOTATION_MARK_WIDTH_HALF > x_date_value + self._ANNOTATION_MARK_WIDTH_HALF or \
                                            middle_time_value_px + self._ANNOTATION_MARK_WIDTH_HALF < x_date_value - self._ANNOTATION_MARK_WIDTH_HALF) and \
                                            (abs(x_date_value - middle_time_value_px) >= self._ANNOTATION_MARK_WIDTH_HALF * 2 + 1):
                                        # annotation markers do not overlap
                                        diff = self._RANGE_ANNOTATION_LINE_HEIGHT_HALF
                                    else:
                                        # annotation markers overlap
                                        diff = self._ANNOTATION_MARK_HEIGHT + self._RANGE_ANNOTATION_LINE_HEIGHT_HALF
                                if abs(value_px - pixels) < diff:

                                    # move current annotation above overlapping annotation to minimal distance
                                    value_px = pixels + self._RANGE_ANNOTATION_LINE_HEIGHT_HALF + self._ANNOTATION_MARK_HEIGHT + 2
                                    # new value is calculated. check again
                                    calculated = False
                                    break
                            if not calculated:
                                break
                        if not calculated:
                            break
                    if not calculated:
                        break

        return value_px

    def _none_to_no_value(self, val):
        """
        replace None with special ChartDirector value NoValue
        """
        if val is None:
            return NoValue
        return val

    def _pack_plot_area(self):
        """
        pack plot area to fix drawing elements outside the chart image size
        """
        if self.settings['type'] != 'thumbnail':
            right_shift = left_shift = self._plot_area_width_margin

            legend_height = 0
            if self.layout_data['include_legend_ind'] == 'Y':
                self.c.layoutLegend()
                legend_height = self.layout_data['legend_y_coord'] + self.legendbox.getHeight() + 15

            if self.layout_data['include_x_axis_label_ind'] != 'Y':
                right_shift = left_shift = 5

            self.c.packPlotArea(left_shift, legend_height, self.c.getWidth() - right_shift, self.c.getHeight() - 15)
            self.c.layoutAxes()

    def _margins_for_bars(self, bar_count):
        """
        calculate margin(s) if first or/and last bar are set
        """
        left_margin = 0
        right_margin = 0
        if self._bars_in_group_count:
            w = self._real_chart_area_width * (1.0 - self.layout_data['bar_group_gap']) / bar_count
            margin = math.ceil(w / 2.0 + w * self.layout_data['bar_group_gap'] / 2.0)

            if self._has_first_bar:
                left_margin = margin
            if self._has_last_bar:
                right_margin = margin

        return int(round(left_margin)), int(round(right_margin))

    def _set_x_axis_margin(self, right_margin, left_margin):
        """
        set x axis right left margins
        """
        self.c.xAxis().setMargin(right_margin, left_margin)

    def _set_bars_gap(self, bar_layer):
        """
        set gaps between bars and sub bars groups
        """

        if self._bars_in_group_count:


            if not self.layout_data['bar_group_gap']:
                bar_group_gap = TouchBar
            else:
                if self.settings['type'] == 'thumbnail':
                    self.layout_data['bar_group_gap'] /= 2 
                bar_group_gap = self.layout_data['bar_group_gap'] = float(self.layout_data['bar_group_gap'])

            if not self.layout_data['bar_gap']:
                bar_gap = TouchBar
            else:
                bar_gap = self.layout_data['bar_gap'] = float(self.layout_data['bar_gap'])

            bar_layer.setBarGap(bar_group_gap, bar_gap)


    def _create_plot_area(self):
        """
        create and set properties of charting area
        """
        self.c = XYChart(
                    self.layout_data['chart_object_size_x'],
                    self.layout_data['chart_object_size_y'],
                    FontManager.get_db_color(self.layout_data['background_color']),
                    FontManager.get_db_color(self.layout_data['border_color']),
                    0)

        # make pretty fonts
        self.c.setAntiAlias(True, AntiAlias)

        # set plot area background color
        if self.layout_data['plot_area_background_color_ind'] == 'Y':
            plot_area_bg_color = FontManager.get_db_color(self.layout_data['plot_area_background_color'])
        else:
            plot_area_bg_color = Transparent

        # set showing grid parameters
        if self.layout_data['show_plot_area_grid_ind'] == 'Y':
            plot_area_horizontal_grid_color = FontManager.get_db_color(self.layout_data['plot_area_horizontal_grid_color'])
            plot_area_vertical_grid_color = FontManager.get_db_color(self.layout_data['plot_area_vertical_grid_color'])
        else:
            plot_area_horizontal_grid_color = Transparent
            plot_area_vertical_grid_color = Transparent

        # set plot area
        self.c.setPlotArea(self.layout_data['plot_area_x_coord'],
                        self.layout_data['plot_area_y_coord'],
                        self.layout_data['plot_area_width'],
                        self.layout_data['plot_area_height'],
                        plot_area_bg_color,
                        -1,
                        Transparent,
                        plot_area_horizontal_grid_color,
                        plot_area_vertical_grid_color)

        # set legend parameters
        if self.layout_data['include_legend_ind'] == 'Y':
            self.legend_font = FontManager.get_db_font(self.layout_data['legend_font'])
            self.legend_font_size = self.layout_data['legend_font_size']
            legend_background_color = FontManager.get_db_color(self.layout_data['legend_background_color'])

            if self.settings['type'] == 'preview':
                self.legend_font_size = 8

            self.legendbox = self.c.addLegend(self.layout_data['legend_x_coord'], self.layout_data['legend_y_coord'], 0, self.legend_font, self.legend_font_size)
            self.legendbox.setBackground(legend_background_color)
            self.legendbox.setKeyBorder(Transparent)
            self.legendbox.setKeySize(30, 4)
        else:
            self.legendbox = None

        # set chart title
        if self.layout_data['include_title_ind'] == 'Y':
            self.c.addTitle(
                       self.layout_data['name'],
                       FontManager.get_db_font(self.layout_data['title_font']),
                       self.layout_data['title_font_size'],
                       self.layout_data['title_font_color'])

    def _set_shape(self, shape_conf):
        """
        set ChartDirector shape by shape id
        """
        shape_list = dict()
        shape_list[u'CircleShape'] = CircleShape
        shape_list[u'SquareShape'] = SquareShape
        shape_list[u'DiamondShape'] = DiamondShape
        shape_list[u'TriangleShape'] = TriangleShape
        shape_list[u'InvertedTriangleShape'] = InvertedTriangleShape
        shape_list[u'RightTriangleShape'] = RightTriangleShape
        shape_list[u'LeftTriangleShape'] = LeftTriangleShape
        shape_list[u'StarShape'] = StarShape
        shape_list[u'PolygonShape'] = PolygonShape
        shape_list[u'CrossShape'] = CrossShape
        shape_list[u'Cross2Shape'] = Cross2Shape
        #if shape_list.has_key(shape_conf['chartdirector_shape_id']):
        if shape_conf['chartdirector_shape_id'] in shape_list:
            if shape_conf['chartdirector_shape_id'] == u'StarShape' or shape_conf['chartdirector_shape_id'] == u'PolygonShape':
                shape = shape_list[shape_conf['chartdirector_shape_id']](shape_conf['side_count'])
            elif shape_conf['chartdirector_shape_id'] == u'CrossShape' or shape_conf['chartdirector_shape_id'] == u'Cross2Shape':
                shape = shape_list[shape_conf['chartdirector_shape_id']](shape_conf['width'])
            else:
                shape = shape_list.get(shape_conf['chartdirector_shape_id'])
        else:
            shape = CircleShape

        return shape

    def _set_x_axis_props(self):
        """
        set x axis properties
        """

        self._prepare_x_axis_charting_data()
        # vertical font for flipped axes
        if self.layout_data['flip_x_and_y_axis'] == 'Y':
            self.x_axis_prop['label_font_angle'] = 0
        else:
            # rotate x axis labels for long labels
            self.x_axis_prop['label_font_angle'] = 90
            if self.settings['type'] == 'preview' and any(len(label) > 10 for label in self._x_axis_labels):
                self.x_axis_prop['label_font_angle'] = 60

        # set x axis label
        if self.layout_data['include_x_axis_label_ind'] == 'Y':
            #if self.layout_data.has_key('x_axis_label_font') and self.layout_data['x_axis_label_font']:
            if 'x_axis_label_font' in self.layout_data and self.layout_data['x_axis_label_font']:
                self.x_axis_prop['label_font'] = FontManager.get_db_font(self.layout_data['x_axis_label_font'])
            else:
                self.x_axis_prop['label_font'] = FontManager.get_default_font()
            self.x_axis_prop['label_font_size'] = self.layout_data['x_axis_label_font_size']
            self.x_axis_prop['label_font_color'] = FontManager.get_db_color(self.layout_data['x_axis_label_font_color'])

            if self.settings['type'] == 'preview':
                self.x_axis_prop['label_font_size'] = 8

            self._set_axis_properties(self.c.xAxis, self.x_axis_prop, self.layout_data['x_axis_title'])
        else:
            self.x_axis_prop['label_font'] = FontManager.get_default_font()
            self.x_axis_prop['label_font_size'] = 8
            self.c.xAxis().setColors(Transparent)

    def _set_y_axis_props(self):
        """
        set y axis properties
        """
        self.y_axis_prop['label_font_angle'] = 0

        # vertical font for flipped axes
        if self.layout_data['flip_x_and_y_axis'] == 'Y':
            self.y_axis_prop['label_font_angle'] = 90

        if self.layout_data['include_y_axis_label_ind'] == 'Y':
            self.y_axis_prop['label_font'] = FontManager.get_db_font(self.layout_data['y_axis_label_font'])
            self.y_axis_prop['label_font_size'] = self.layout_data['y_axis_font_size']
            self.y_axis_prop['label_font_color'] = FontManager.get_db_color(self.layout_data['y_axis_font_color'])

            if self.settings['type'] == 'preview':
                self.y_axis_prop['label_font_size'] = 8

            # set data for primary y axis
            self._set_axis_properties(self.c.yAxis, self.y_axis_prop, self.layout_data['y_axis_title_left'])

            # set data for secondary Y axis
            if self._has_y_axis_2:
                prim_color = self.y_axis_prop['label_font_color']
                #if self.layout_data.has_key('y2_axis_font_color'):
                if 'y2_axis_font_color' in self.layout_data:
                    self.y_axis_prop['label_font_color'] = self.layout_data['y2_axis_font_color']

                self._set_axis_properties(self.c.yAxis2, self.y_axis_prop, self.layout_data['y_axis_title_right'])
                self.y_axis_prop['label_font_color'] = prim_color
        else:
            self.y_axis_prop['label_font'] = FontManager.get_default_font()
            self.y_axis_prop['label_font_size'] = 8
            self.c.yAxis().setColors(Transparent)
            self.c.yAxis2().setColors(Transparent)

        # primary Y axis properties
        if self.layout_data['primary_y_axis_display_mask']:
            self._set_y_axis_format(self.c.yAxis, self.layout_data['primary_y_axis_display_mask'])

        # secondary Y axis properties
        if self._has_y_axis_2:
            if self.layout_data['secondary_y_axis_display_mask']:
                self._set_y_axis_format(self.c.yAxis2, self.layout_data['secondary_y_axis_display_mask'])

    def _set_x_axis_date_scale(self):
        """
        set x axis date labels
        """
        self._x_axis_data = [chartTime2(float(header.strftime("%s"))) for header in self.chart_data['orig_header']]
        #remove labels set by auto scaling
        if self._x_axis_data:
            self.c.xAxis().setDateScale(self._x_axis_data[0], self._x_axis_data[-1], list())
        if self.layout_data['include_x_axis_label_ind'] == 'Y':
            for i, v in enumerate(self._x_axis_values):
                self.c.xAxis().addLabel(self._x_axis_values[i], self._x_axis_labels[i])
        self._markup_plot_area()

    def _prepare_x_scale_values(self):
        self.chart_data['x_scale_values'] = [chartTime2(float(x_scale_value.strftime("%s"))) for x_scale_value in self.chart_data['x_scale_values']]

    def _prepare_x_axis_charting_data(self):
        """
        set labels and label step for date x-axis
        """
        self._x_axis_labels = []
        self._x_axis_values = []
        if self.layout_data['include_x_axis_label_ind'] == 'Y':
            len_headers = len(self.chart_data['x_scale_values'])
            # reduce labels amount if needed
            if len_headers > self.layout_data['max_x_axis_labels']:
                label_step, remainder = divmod(len_headers, self.layout_data['max_x_axis_labels'])
                if remainder:
                    label_step += 1
            else:
                label_step = 1

            # set custom labels
            if len_headers:
                for i in xrange(0, len_headers, label_step):
                    self._x_axis_labels.append(self.chart_data['x_scale_labels'][i])
                    self._x_axis_values.append(self.chart_data['x_scale_values'][i])

    def _markup_plot_area(self):
        """
        calculate real chart dimensions in pixels and set axes scaling
        """
        # calculate plot area height in pixels
        self._calculate_real_plot_height()

        self._set_scale_max_min_values(0)
        if self._has_y_axis_2:
            self._set_scale_max_min_values(1)

        # calculate plot area width in pixels
        self._calculate_real_plot_width()

    def _set_y_axes_scaling(self):
        """
        set final scaling for y axes
        """
        # remove y-axis rounding labels so top value can be positioned near to top
        self.c.yAxis().setRounding(False, False)
        if self._has_y_axis_2:
            self.c.yAxis2().setRounding(False, False)
        # final scale axis

        self.c.yAxis().setLinearScale(self._y_min_value[0], self._y_max_value[0], self._ticks[0])

        if self._has_y_axis_2:
            self.c.yAxis2().setLinearScale(self._y_min_value[1], self._y_max_value[1], self._ticks[1])

    def _get_element_color(self, display_type, line_type, color):
        """
        return color for bars and dashed/dotted/solid for lines
        """
        if display_type == 'line' and line_type.lower() == 'dashline':
            return self.c.dashLineColor(color, DashLine)
        elif display_type == 'line' and line_type.lower() == 'dotline':
            return self.c.dashLineColor(color, DotLine)
        else:
            return color

    def _create_layers(self, is_stacked):
        """
        create and enumerate layers, data-set in layers for fetching map coordinates
        set layers: line, spline bar, legend
        """

        # this is simple line layer. LINE_LAYER_IND = 0
        line_layer = self.c.addLineLayer2()
        line_layer.addExtraField2(self.chart_data['even_header'])
        line_layer.setXData(self._x_axis_data)
        line_layer.setLineWidth(self.layout_data['line_width'])

        # this is simple spline layer. this is SPLINE_LAYER_IND = 1
        spline_layer = self.c.addSplineLayer()
        spline_layer.setMonotonicity(MonotonicXY)
        spline_layer.addExtraField2(self.chart_data['even_header'])
        spline_layer.setXData(self._x_axis_data)
        spline_layer.setLineWidth(self.layout_data['line_width'])

        # set line gap type
        if self.layout_data['data_gap_line_type'] == 'dashed':
            line_layer.setGapColor(self.c.dashLineColor(SameAsMainColor, DashLine), 1)
            spline_layer.setGapColor(self.c.dashLineColor(SameAsMainColor, DashLine), 1)
        elif self.layout_data['data_gap_line_type'] == 'solid':
            line_layer.setGapColor(SameAsMainColor, 1)
            spline_layer.setGapColor(SameAsMainColor, 1)

        # this is simple bar layer. BAR_LAYER_IND = 2
        if is_stacked:
            bar_layer = self.c.addBarLayer2(Stack)
        else:
            bar_layer = self.c.addBarLayer2(Side)
        bar_layer.addExtraField2(self.chart_data['even_header'])
        bar_layer.setXData(self._x_axis_data)

        if self.layout_data['bar_shape'] == 'cylindric':
            bar_layer.setBarShape(CircleShape)

        # setting bar soft lighting direction
        if self.layout_data['bar_soft_lighting_direction'] != 'none':
            if self.layout_data['bar_soft_lighting_direction'] == 'top':
                bar_layer.setBorderColor(Transparent, softLighting(Top))

            elif self.layout_data['bar_soft_lighting_direction'] == 'bottom':
                bar_layer.setBorderColor(Transparent, softLighting(Bottom))

            elif self.layout_data['bar_soft_lighting_direction'] == 'left':
                bar_layer.setBorderColor(Transparent, softLighting(Left))

            elif self.layout_data['bar_soft_lighting_direction'] == 'right':
                bar_layer.setBorderColor(Transparent, softLighting(Right))
            else:
                bar_layer.setBorderColor(Transparent)
        else:
            bar_layer.setBorderColor(Transparent)

        # set gaps between bars
        self._set_bars_gap(bar_layer)

        # setting bar soft lighting direction
        if self.layout_data['bar_soft_lighting_direction'] != 'none':
            if self.layout_data['bar_soft_lighting_direction'] == 'top':
                if self.layout_data['flip_x_and_y_axis'] == 'Y':
                    bar_layer.setBorderColor(Transparent, softLighting(Right))
                else:
                    bar_layer.setBorderColor(Transparent, softLighting(Top))
            if self.layout_data['bar_soft_lighting_direction'] == 'bottom':
                if self.layout_data['flip_x_and_y_axis'] == 'Y':
                    bar_layer.setBorderColor(Transparent, softLighting(Left))
                else:
                    bar_layer.setBorderColor(Transparent, softLighting(Bottom))
            if self.layout_data['bar_soft_lighting_direction'] == 'left':
                if self.layout_data['flip_x_and_y_axis'] == 'Y':
                    bar_layer.setBorderColor(Transparent, softLighting(Top))
                else:
                    bar_layer.setBorderColor(Transparent, softLighting(Left))
            if self.layout_data['bar_soft_lighting_direction'] == 'right':
                if self.layout_data['flip_x_and_y_axis'] == 'Y':
                    bar_layer.setBorderColor(Transparent, softLighting(Bottom))
                else:
                    bar_layer.setBorderColor(Transparent, softLighting(Right))

        # this is layer for legend labels, LEGEND_LAYER_IND = 3
        legend_layer = self.c.addLineLayer2()
        legend_layer.setLineWidth(self.layout_data['line_width'])

        # count of created layers
        self.layers_count = self.LEGEND_LAYER_IND + 1

        self.datasets = dict()
        self.datasets[self.LINE_LAYER_IND] = dict()
        self.datasets[self.SPLINE_LAYER_IND] = dict()
        self.datasets[self.BAR_LAYER_IND] = dict()

        self.dataset_ids = dict()
        self.dataset_ids[self.LINE_LAYER_IND] = 0
        self.dataset_ids[self.SPLINE_LAYER_IND] = 0
        self.dataset_ids[self.BAR_LAYER_IND] = 0

        self.line_layer = line_layer
        self.spline_layer = spline_layer
        self.bar_layer = bar_layer
        self.legend_layer = legend_layer

    def _create_bar_layer(self, data, color, axis, description):
        """
        add bar data to chart
        """
        try:
            data_layer = self.bar_layer.addDataSet(data, color)
        except TypeError:
            return None
        self.datasets[self.BAR_LAYER_IND][self.dataset_ids[self.BAR_LAYER_IND]] = description
        self.dataset_ids[self.BAR_LAYER_IND] += 1
        data_layer.setUseYAxis(axis)
        return data_layer

    def _create_line_layer(self, data, line_style, line_width, color, axis, show_data_points, shape, line_description, line_point_description):
        """
        add line data to chart
        """
        if line_style == 'smooth':
            try:
                data_layer = self.spline_layer.addDataSet(data, color)
            except TypeError:
                return None
            curr_line_layer_ind = self.SPLINE_LAYER_IND
        else:
            try:
                data_layer = self.line_layer.addDataSet(data, color)
            except TypeError:
                return None
            curr_line_layer_ind = self.LINE_LAYER_IND

        data_layer.setUseYAxis(axis)

        # set custom line width
        if line_width is not None:
            data_layer.setLineWidth(line_width)

        if show_data_points:
            # set line layer point shape
            data_layer.setDataSymbol(shape, self.layout_data['line_data_point_dot_size'], color, color)

            self.datasets[curr_line_layer_ind][self.dataset_ids[curr_line_layer_ind]] = line_description
            self.dataset_ids[curr_line_layer_ind] += 1

            # for line data layers with symbols points add scatter layer for exact coordinates of points
            data_points_layer = self.c.addScatterLayer(self._x_axis_data, data, '', shape, self.layout_data['line_data_point_dot_size'] + 1, Transparent, Transparent)
            data_points_layer.addExtraField2(self.chart_data['even_header'])
            data_points_layer.setUseYAxis(axis)

            # add layer for points
            self.datasets[self.layers_count] = dict()
            self.datasets[self.layers_count][0] = line_point_description
            self.dataset_ids[self.layers_count] = 0
            self.layers_count += 1
        else:
            # for line non data layers or for data layers without symbols points
            self.datasets[curr_line_layer_ind][self.dataset_ids[curr_line_layer_ind]] = line_point_description
            self.dataset_ids[curr_line_layer_ind] += 1

        return data_layer

    def _calc_annotations(self):
        """
        calculate value for selected annotations
        """

        fit_size = False
        i = 0
        while not fit_size:
            i += 1
            fit_size = True
            heights = []
            for ind, range_annotation in enumerate(self._range_annotations):
                # calculate value
                recalculated_value_in_pixels = self._calc_annotation_value(range_annotation, ind)

                heights.append(recalculated_value_in_pixels)
                self._range_annotations[ind]['value'] = self._scale_pixels_to_y_value(recalculated_value_in_pixels, range_annotation['axis'])

            max_height = max(heights) + self._ANNOTATION_MARK_HEIGHT

            if int(max_height) > int(self._real_chart_area_height):
                fit_size = False
                y_max_value = self._scale_pixels_to_y_value(max_height, 0)
                self._real_y_max_value[0] = self._recalculate_real_max_y_value(0, y_max_value)
                self._set_scale_max_min_values(0)

                if self._has_y_axis_2:
                    y_max_value = self._scale_pixels_to_y_value(max_height, 1)
                    self._real_y_max_value[1] = self._recalculate_real_max_y_value(1, y_max_value)
                    self._set_scale_max_min_values(1)

                self._calculate_real_plot_width()

    def _draw_range_annotation(self, range_annotation):
        """
        draw range annotations for charting element
        """
        if range_annotation['axis'] == 1:
            axis = self.c.yAxis2()
        else:
            axis = self.c.yAxis()
        # set range line end points
        left_point = chartTime2(float(range_annotation['from_time'].strftime('%s')))
        left_hds = [left_point]
        right_point = chartTime2(float(range_annotation['to_time'].strftime('%s')))
        right_hds = [right_point]

        # set middle point for annotation marker
        middle_point = chartTime2(float(range_annotation['middle_time'].strftime('%s')))
        middle_hds = [middle_point]

        # scale line. need for scaling line image
        scale = right_point - left_point

        value = range_annotation['value']
        if value is None:
            value = NoValue
        markData = [value]

        # middle layer for annotation marker
        middle_scat = self.c.addScatterLayer(middle_hds, markData, '', 0)

        # left right end points layers
        left_scat = self.c.addScatterLayer(left_hds, markData, '', 0)
        right_scat = self.c.addScatterLayer(right_hds, markData, '', 0)

        # central line layer
        line_scat = self.c.addScatterLayer(middle_hds, markData, '', 0)
        line_symbol = self.config.resource_file('ma_line.png')
        line_scat.getDataSet(0).setDataSymbol2(line_symbol)
        line_scat.setUseYAxis(axis)
        x_scale = [scale]
        y_scale = [3]
        line_scat.setSymbolScale(x_scale, XAxisScale, y_scale, PixelScale)

        # draw left/right end point images
        if range_annotation['left_marker']:
            left_annot_symbol = self.config.resource_file('ma_tick.png')
            left_scat.getDataSet(0).setDataSymbol2(left_annot_symbol)
        if range_annotation['right_marker']:
            right_annot_symbol = self.config.resource_file('ma_tick.png')
            right_scat.getDataSet(0).setDataSymbol2(right_annot_symbol)

        # annotation marker
        middle_scat.getDataSet(0).setDataSymbol2(self.config.resource_file('annotation.png'))

        left_scat.setUseYAxis(axis)
        right_scat.setUseYAxis(axis)
        middle_scat.setUseYAxis(axis)
        line_scat.setUseYAxis(axis)

        text_box = middle_scat.setDataLabelStyle(FontManager.get_default_bold_font(), 8)
        text_box.setFontColor(0xFFFFFF)
        text_box.setAlignment(BottomCenter)
        text_box.setPos(-10, -5)
        text_box.setWidth(20)
        text_box.setHeight(20)
        text_box.setZOrder(GridLinesZ)

        middle_scat.setDataLabelFormat("{dsdiField0}")

        middle_scat.moveFront()
        left_scat.moveFront()
        right_scat.moveFront()
        line_scat.moveFront()

        annot_marks_indexes = [str(range_annotation['index'])]
        middle_scat.addExtraField(annot_marks_indexes)

    def _collect_range_annotations(self, range_annotations, axis, additional_data):
        """
        collect list of range annotations
        """
        if range_annotations:
            for range_annotation in range_annotations:
                range_annotation['axis'] = axis

                for k, v in additional_data.iteritems():
                    range_annotation[k] = v

                self._range_annotations.append(range_annotation)

    def _draw_point_annotations(self, point_annotations, data, axis, curr_axis, display_type, bar_ind, is_x_axis_date, description):
        """
        draw point annotations for charting element
        """
        if not point_annotations:
            return

        # get values for scatter layer, remove values without annotations)
        prepare_scatter_value = lambda i, item: data[i] if item else NoValue
        scatter_values = [prepare_scatter_value(i, ann) for i, ann in enumerate(point_annotations)]

        # add data to scatter layer
        scat_layer = self.c.addScatterLayer(list(), scatter_values, '', 0)
        scat_layer.setUseYAxis(axis)

        if display_type == 'bar':
            # if there is more then one bar in group x axis position of annotation must be adjusted

            # middle of bars
            middle = (self._bars_in_group_count - 1) / 2.0

            # scale of x axis. actually it is distance between middles of bar groups
            #scale = (self._x_axis_data[-1] - self._x_axis_data[0]) / (len(self._x_axis_data) - 1)

            if is_x_axis_date:
                scale = (self._x_axis_data[-1] - self._x_axis_data[0]) / (len(self._x_axis_data))
            else:
                scale = (self._x_axis_data[-1] - self._x_axis_data[0]) / (len(self._x_axis_data) - 1)

            # with of bar group
            bar_group_width = (1 - self.layout_data['bar_group_gap']) * scale

            # with of one bar
            bar_width = bar_group_width / float(self._bars_in_group_count)

            # with of gap between bars in group
            bar_gap = bar_width * self.layout_data['bar_gap']

            if bar_ind < middle:
                # bar is to the left of the middle of bar group
                shift = - (middle - bar_ind) * bar_width * (1 + bar_gap * (middle - bar_ind))
            elif bar_ind > middle:
                # bar is to the right of the middle of bar group
                shift = (bar_ind - middle) * bar_width * (1 + bar_gap * (bar_ind - middle))
            else:
                # bar is in the middle of bar group
                shift = 0.0

            x_axis_data = [val + shift for val in self._x_axis_data]
            if is_x_axis_date:
                scale_shift = shift / (self._x_axis_data[-1] - self._x_axis_data[0]) * (self._x_axis_time_values[-1] - self._x_axis_time_values[0])
                x_axis_time = [val + scale_shift for val in self._x_axis_time_values]
        else:
            # no need to adjust position
            x_axis_data = self._x_axis_data
            if is_x_axis_date:
                x_axis_time = self._x_axis_time_values

        scat_layer.setXData(x_axis_data)
        if is_x_axis_date:
            # save data for checking overlapping
            annotations_values = [data[i] if a else None for i, a in enumerate(point_annotations)]
            self._annotations[curr_axis].append({'x_axis_data': x_axis_time, 'values': annotations_values})

        # set annotation image as DataSymbol
        if self.layout_data['flip_x_and_y_axis'] == 'Y' and data['display_type'] == 'bar':
            scat_layer.getDataSet(0).setDataSymbol2(self.config.resource_file('annotation_flipped_bar.png'))
        else:
            scat_layer.getDataSet(0).setDataSymbol2(self.config.resource_file('annotation.png'))

        scat_layer.setDataLabelFormat("{dsdiField0}")

        annot_marks_indexes = map(lambda annot: '%s' % annot if annot  else '', point_annotations)

        scat_layer.addExtraField(annot_marks_indexes)

        scat_layer.moveFront()

        text_box = scat_layer.setDataLabelStyle(FontManager.get_default_bold_font(), 8)
        text_box.setFontColor(0xFFFFFF)

        if self.layout_data['flip_x_and_y_axis'] == 'Y' and display_type == 'bar':
            text_box.setPos(3, -10)
            text_box.setAlignment(5)
        else:
            text_box.setPos(-10, -5)
            text_box.setAlignment(BottomCenter)
        text_box.setWidth(20)
        text_box.setHeight(20)
        text_box.setZOrder(GridLinesZ)

        self.datasets[self.layers_count] = dict()
        self.datasets[self.layers_count][0] = description
        self.dataset_ids[self.layers_count] = 0
        self.layers_count += 1
