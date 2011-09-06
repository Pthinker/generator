#-*- coding: utf-8 -*-

from metric.conf import ConfigReader
from metric.file_man.jfiles import JChartFile
from font_manager import FontManager
from pychartdir import *
import urllib
import re


class BasePieChart:
    annotations_map = []
    resized_preview = None
    resized_preview_filename = ''

    def __init__(self, element_id, report_data_set_chart_id, report_data_set_instance_id, data, settings, jfile):
        self.annotations_map = []
        self.config = ConfigReader()
        self.file_man = JChartFile()
        self.element_id = element_id
        self.report_data_set_chart_id = report_data_set_chart_id
        self.report_data_set_instance_id = report_data_set_instance_id
        self.chart_data = data['chart_data']
        self.layout_data = data['layout_data']
        self.data = data
        self.settings = settings
        self._jfile = jfile

        #self.create_chart()

        self.datasets = {}

    def create_chart(self):
        c = PieChart(self.layout_data['chart_object_size_x'],
                     self.layout_data['chart_object_size_y'],
                     FontManager.get_db_color(self.layout_data['background_color']),
                     FontManager.get_db_color(self.layout_data['border_color']),
                     0)

        # make pretty fonts
        c.setAntiAlias(True, AntiAlias)

        labels = self.chart_data['formatted_header']

        try:
            c.setData(self.chart_data['data'], labels)
        except TypeError:
            raise Exception("Chart %s misconfigured. Sector Values must contain numeric values." % self.report_data_set_chart_id)

        # colors for sectors for the pie chart
        if self.chart_data['colors']:
            c.setColors2(DataColor, self.chart_data['colors'])

        # set chart title
        if self.layout_data['include_title_ind'] == 'Y':
            c.addTitle(self.layout_data['name'],
                        FontManager.get_db_font(self.layout_data['title_font']),
                        self.layout_data['title_font_size'],
                        self.layout_data['title_font_color'])
        # define center coordinates
        if not self.layout_data['pie_chart_center_x_coord']:
            self.layout_data['pie_chart_center_x_coord'] = self.layout_data['chart_object_size_x'] // 2

        if not self.layout_data['pie_chart_center_y_coord']:
            self.layout_data['pie_chart_center_y_coord'] = self.layout_data['chart_object_size_y'] // 2

        # define radius
        if not self.layout_data['pie_chart_radius']:
            if self.layout_data['chart_object_size_y'] > self.layout_data['chart_object_size_x']:
                smallest_side = self.layout_data['chart_object_size_x']
            else:
                smallest_side = self.layout_data['chart_object_size_y']
            self.layout_data['pie_chart_radius'] = int(smallest_side // 2 * 0.7)
            # reduce radius for preview chart
            if self.settings['type'] == 'preview':
                #self.layout_data['pie_chart_radius'] = self.layout_data['pie_chart_radius'] * 0.9
                self.layout_data['pie_chart_radius'] *= 0.9

        # set pie position and radius
        c.setPieSize(self.layout_data['pie_chart_center_x_coord'], self.layout_data['pie_chart_center_y_coord'], self.layout_data['pie_chart_radius'])

        # 3D chart settings
        if self.layout_data['pie_chart_type'] == '3D':
            # if depth not specified use default
            if not self.layout_data['pie_chart_depth']:
                self.layout_data['pie_chart_depth'] = -1
            # if start angle not specified use default
            if not self.layout_data['pie_chart_display_angle']:
                self.layout_data['pie_chart_display_angle'] = -1
            c.set3D(self.layout_data['pie_chart_depth'], self.layout_data['pie_chart_display_angle'])

        # start angle
        if not self.layout_data['pie_chart_start_angle']:
            self.layout_data['pie_chart_start_angle'] = 0
        c.setStartAngle(self.layout_data['pie_chart_start_angle'])

        # set labels position formats
        if self.settings['type'] == 'preview':
            label_pos = 5
        else:
            label_pos = 15
        c.setLabelLayout(SideLayout, label_pos)

        # label font color
        if self.settings['type'] == 'thumbnail':
            self.layout_data['pie_chart_sector_label_font_color'] = Transparent
        else:
            if self.layout_data['pie_chart_sector_label_font_color']:
                # use specified color
                self.layout_data['pie_chart_sector_label_font_color'] = FontManager.get_db_color(self.layout_data['pie_chart_sector_label_font_color'])
            else:
                # same as sector color
                self.layout_data['pie_chart_sector_label_font_color'] = SameAsMainColor

        t = c.setLabelStyle(FontManager.get_db_font(self.layout_data['pie_chart_sector_label_font']),
                            self.layout_data['pie_chart_sector_label_font_size'],
                            self.layout_data['pie_chart_sector_label_font_color'])
        if self.settings['type'] == 'preview' or self.settings['type'] == 'large':
            max_label_width = (self.layout_data['chart_object_size_x'] - self.layout_data['pie_chart_radius'] * 2 - label_pos * 2) / 2
            t.setMaxWidth(max_label_width)

        if self.settings['type'] == 'thumbnail':
            fill_label_color = Transparent
            sector_label_border_color = Transparent
        else:
            if self.layout_data['use_sector_color_for_Label_fill_ind'] == 'Y':
                # same as sector color
                fill_label_color = SameAsMainColor
            else:
                # no background
                fill_label_color = Transparent
            if self.layout_data['sector_label_border_color']:
                # use specified color
                sector_label_border_color = FontManager.get_db_color(self.layout_data['sector_label_border_color'])
            else:
                # no border
                sector_label_border_color = Transparent
        # set label background and border color
        t.setBackground(fill_label_color, sector_label_border_color)

        # rounded corners of labels
        if self.layout_data['sector_label_corner_style'] == 'Rounded':
            t.setRoundedCorners()

        # join line format
        if self.settings['type'] == 'thumbnail':
            # do not draw line
            join_line_color = Transparent
        else:
            if self.layout_data['sector_label_join_line_color']:
                # use specified color
                join_line_color = FontManager.get_db_color(self.layout_data['sector_label_join_line_color'])
            else:
                # use sector color
                join_line_color = SameAsMainColor

        c.setJoinLine(join_line_color)

        # shading format
        if self.layout_data['pie_chart_shading'] == 'Rounded Edge':
            shading = RoundedEdgeShading
        elif self.layout_data['pie_chart_shading'] == 'Flat Gradient' and self.layout_data['pie_chart_type'] == '3D':
            shading = FlatShading
        else:
            shading = DefaultShading

        # explode sectors
        if (self.layout_data['explode_all_pie_sectors'] == 'all' or
                (self.layout_data['explode_all_pie_sectors'] == '2D' and self.layout_data['pie_chart_type'] == '2D') or
                (self.layout_data['explode_all_pie_sectors'] == '3D' and self.layout_data['pie_chart_type'] == '3D')):
            if not self.layout_data['pie_sectors_explode_width']:
                self.layout_data['pie_sectors_explode_width'] = 0
            # reduce border width for small images
            if self.settings['type'] != 'large':
                #self.layout_data['pie_sectors_explode_width'] = self.layout_data['pie_sectors_explode_width'] / 2
                self.layout_data['pie_sectors_explode_width'] /= 2

            c.setExplode(-1, self.layout_data['pie_sectors_explode_width'])
            self.layout_data['pie_chart_sector_border_width'] = 0
            self.layout_data['sector_edge_line_color'] = ''

        # sector border(edge) color
        if self.layout_data['sector_edge_line_color']:
            # use specified color
            edge_line_color = FontManager.get_db_color(self.layout_data['sector_edge_line_color'])
        else:
            # use sector color
            edge_line_color = SameAsMainColor

        # sector border(edge) width
        if not self.layout_data['pie_chart_sector_border_width']:
            self.layout_data['pie_chart_sector_border_width'] = 0

        # reduce border with for small images
        if self.settings['type'] != 'large':
            #self.layout_data['pie_chart_sector_border_width'] = self.layout_data['pie_chart_sector_border_width'] / 2
            self.layout_data['pie_chart_sector_border_width'] /= 2

        c.setSectorStyle(shading, edge_line_color, self.layout_data['pie_chart_sector_border_width'])

        # number of digits after point for % value
        if not self.layout_data['sector_value_pct_precision_digits']:
            self.layout_data['sector_value_pct_precision_digits'] = 0

        # label color
        if self.layout_data['pie_chart_sector_value_font_color']:
            self.layout_data['pie_chart_sector_value_font_color'] = FontManager.get_db_color(self.layout_data['pie_chart_sector_value_font_color'])
        else:
            self.layout_data['pie_chart_sector_value_font_color'] = None

        # label format
        if self.layout_data['show_sector_value_ind'] == 'Y':
            # show label, percentage and value in label
            c.addExtraField(self.chart_data['formatted_data'])
            label_format = "<*block*><*font=%s,size=%s,color=%s*>{label}<*br*>{field0} ({percent|%s}%%)<*/*>"
        else:
            # show label and percentage in label
            label_format = "<*block*><*font=%s,size=%s,color=%s*>{label}<*br*>({percent|%s}%%)<*/*>"

        # sector value font size
        if not self.layout_data['pie_chart_sector_value_font_size'] or self.layout_data['pie_chart_sector_value_font_size'] < 3:
            self.layout_data['pie_chart_sector_value_font_size'] = 8

        #reduce label font size for preview images
        if self.settings['type'] == 'preview':
            self.layout_data['pie_chart_sector_value_font_size'] = 8

        if self.layout_data['pie_chart_sector_value_font_color'] is not None:
            # if label color is specified, use it for all labels
            c.setLabelFormat(label_format % (
                                    FontManager.get_db_font(self.layout_data['pie_chart_sector_value_font']),
                                    self.layout_data['pie_chart_sector_value_font_size'],
                                    self.layout_data['pie_chart_sector_value_font_color'],
                                    self.layout_data['sector_value_pct_precision_digits']))
        else:
            # if label color is not specified, use color of sector
            for i, color in enumerate(self.chart_data['colors']):
                c.sector(i).setLabelFormat(label_format % (
                                    FontManager.get_db_font(self.layout_data['pie_chart_sector_value_font']),
                                    self.layout_data['pie_chart_sector_value_font_size'],
                                    hex(color),
                                    self.layout_data['sector_value_pct_precision_digits']))

        # draw annotations
        if self.settings['type'] == 'large':
            c.layout()
            index = 1
            for i, header in enumerate(self.chart_data['formatted_header']):
                if self.chart_data['annotations'][header]:
                    sector = c.getSector(i)
                    coord_string = sector.getLabelCoor()

                    content = urllib.unquote(coord_string)

                    res = re.findall(r'shape="rect" coords="(.+?),(.+?),(.+?),(.+?)"', content)
                    coords = res[0]

                    x_coord = (int(coords[2]) + int(coords[0])) / 2
                    y_coord = int(coords[1]) + 25

                    annot_marker = "<*img=%s*>" % self.config.resource_file('annotation.png')
                    c.addText(x_coord, y_coord, annot_marker, "", 0, 0x0, BottomCenter)
                    c.addText(x_coord, y_coord - 32, str(index), FontManager.get_default_bold_font(), 10, 0xffffff, BottomCenter)
                    for annot in self.chart_data['annotations'][header]:
                        annot['index'] = index
                        annot['shape'] = 'rect'
                        annot['chart_by_column_value'] = header
                        annot['chart_element_identifier'] = ''
                        annot['value'] = self.chart_data['formatted_data'][self.chart_data['header'].index(header)]
                        marker_coors = [x_coord - 9, y_coord - 47, x_coord + 10, y_coord - 29]
                        annot['coords'] = ','.join(map(str, marker_coors))
                        annot['annotation_interval'] = 'point'
                        annot['start_time'] = ''
                        annot['finish_time'] = ''
                        annot['raw_start_time'] = ''
                        annot['raw_finish_time'] = ''
                        self.annotations_map.append(annot)
                    index += 1

        filename = ''
        if self.settings['type'] == 'large':
            filename = self._jfile.get_chart_file_name(self.report_data_set_chart_id, self.report_data_set_instance_id)
        elif self.settings['type'] == 'thumbnail':
            filename = self._jfile.get_thumbnail_file_name()
        elif self.settings['type'] == 'preview':
            filename = self._jfile.get_preview_file_name()

        if not c.makeChart(filename):
            raise Exception("ChartDirector cannot create image file %s for chart %s." % (filename, self.report_data_set_chart_id))
        self.file_man.change_perm(filename)

        self.c = c

        if self.settings['type'] == 'large':
            self.create_map_file()
            if self.settings['is_index']:
                self.create_resized_preview()

    def create_resized_preview(self):
        resized_preview = self.c.makeChart3()
        resized_preview.resize(self.layout_data['preview']['chart_object_size_x'], self.layout_data['preview']['chart_object_size_y'], BoxFilter)
        resized_preview.setSize(self.layout_data['preview']['chart_object_size_x'], self.layout_data['preview']['chart_object_size_y'])

        filename = self._jfile.get_resized_preview_file_name()

        resized_preview.outPNG(filename)
        self.file_man.change_perm(filename)

    def create_map_file(self):
        map = self.get_parsed_map()
        data = []
        for v in map:
            data.append({'shape': v['shape'],
                        'coords': v['coords'],
                        'name': v['name'],
                        'meas_index': v['meas_index'],
                        'raw_meas_index': '',
                        'value': v['value']})

        self._jfile.make_chart_file(data, self.annotations_map, self.report_data_set_chart_id, self.report_data_set_instance_id, 'pie', 'non date')

    def get_parsed_map(self):
        content = self.c.getHTMLImageMap('url', 'sector={sector}&label={label}&percent={percent}')
        content = urllib.unquote(content)
        res = re.findall(r'(<area shape="poly" coords="(.+?)" href="url\?sector=(.+?)&label=(.+?)&percent=(.+?)">)', content)
        result = list({'shape': "poly",
                       'coords': v[1],
                       'name': self.chart_data['formatted_header'][int(v[2])],
                       'meas_index': self.chart_data['formatted_data'][int(v[2])],
                       'value': v[4] + '%'} for v in res)
        return result
