#-*- coding: utf-8 -*-

from metric.conf import ConfigReader
import os.path


class FontManager:
    _inited = False
    default_font = 'arial.ttf'
    default_bold_font = 'arialbd.ttf'

    def __init__(self):
        config = ConfigReader()
        FontManager.font_path = config.font_path

    @classmethod
    def _get_font_file(cls, font):
        config = ConfigReader()
        full_font_path = os.path.join(config.font_path, font)
        if os.path.exists(full_font_path):
            return full_font_path
        return font

    @classmethod
    def get_db_font(cls, db_font):
        font = cls._get_font_file(db_font['chartdirector_font_file_name'])
        return font

    @classmethod
    def get_default_font(cls):
        font = cls._get_font_file(FontManager.default_font)
        return font

    @classmethod
    def get_default_bold_font(cls):
        return FontManager._get_font_file(FontManager.default_bold_font)

    @classmethod
    def get_db_color(cls, db_color):
        return int('0x' + db_color[1:], 16)
