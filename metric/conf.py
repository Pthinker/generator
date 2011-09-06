#-*- coding: utf-8 -*-
import ConfigParser
import os.path
import sys
from logger.logger import MLogger
class ConfigReader:
    _inited = False
    _generator_root_path = None
    db = None
    db_host = None
    db_user = None
    db_passwd = None
    db_root_user = None
    db_root__passwd = None

    report_root = None
    thumbnail_root = None
    preview_root = None
    metric_root = None
    multimetric_root = None
    font_path = None
    resource_path = None
    fetch_broker = None
    file_owner_uid = None
    file_owner_gid = None
    use_encryption = None
    encryption_password_path = None

    aqb_metadata_manager_url = None

    def __init__(self):
        if not ConfigReader._inited:
            self.path = os.path.abspath(os.path.dirname(__file__))
            self.config = ConfigParser.SafeConfigParser()
            logger = MLogger('config')
            self._logger = logger.get_logger()

            # open config
            try:
                ConfigReader._generator_root_path = os.path.split(os.path.split(os.path.abspath( __file__ ))[0])[0]
                self.config.read(os.path.join(self._generator_root_path, 'cfg', 'config.ini'))
            except IOError:
                self._logger.error("no config file error")
                sys.exit(1)
            except ConfigParser.MissingSectionHeaderError:
                self._logger.error("config file reading section error")
                sys.exit()
                pass

            self.parse_cfg()
            ConfigReader._inited = True

    def parse_item(self, section, item):
        try:
            val = self.config.get(section, item)
        except ConfigParser.NoOptionError:
            self._logger.error("no '%s' in config file" % item)
            sys.exit(1)
        except ConfigParser.NoSectionError:
            self._logger.error("no section '%s'" % section)
            sys.exit(1)
        return val

    def resource_file(self, file_name):
        return os.path.join(ConfigReader.resource_path, file_name)

    def parse_cfg(self):
        # db properties
        ConfigReader.db_host = self.parse_item('MySQL', 'host')
        ConfigReader.db_user = self.parse_item('MySQL', 'user')
        ConfigReader.db_passwd = self.parse_item('MySQL', 'passwd')
        ConfigReader.db = self.parse_item('MySQL', 'db')

        try:
            ConfigReader.db_root_user = self.config.get('MySQL', 'root_user')
            ConfigReader.db_root_passwd = self.config.get('MySQL', 'root_passwd')
        except ConfigParser.NoOptionError:
            ConfigReader.db_root_user = ConfigReader.db_user
            ConfigReader.db_root_passwd = ConfigReader.db_passwd

        # data paths
        ConfigReader.report_root = self.parse_item('Path', 'report_root')
        ConfigReader.thumbnail_root = self.parse_item('Path', 'thumbnail_root')
        ConfigReader.preview_root = self.parse_item('Path', 'preview_root')
        ConfigReader.metric_root = self.parse_item('Path', 'metric_root')
        ConfigReader.multimetric_root = self.parse_item('Path', 'multimetric_root')

        # resource dir path
        ConfigReader.resource_path = os.path.join(ConfigReader._generator_root_path, 'resource')

        # path to font dir
        ConfigReader.font_path = os.path.join(ConfigReader._generator_root_path, 'resource', 'fonts')

        # data collector path
        ConfigReader.fetch_broker = self.parse_item('Path', 'fetch_broker')

        # aqb path
        ConfigReader.aqb_metadata_manager_url = self.parse_item('Path', 'aqb_metadata_manager_url')

        # include path to ChartDirector to import path
        sys.path.append(self.parse_item('Path', 'chartdirector_lib'))

        # default file owner
        file_owner = self.parse_item('Permissions', 'file_owner')
        owner = pwd.getpwnam(file_owner)
        ConfigReader.file_owner_uid = owner[2]
        ConfigReader.file_owner_gid = owner[3]

        # use or not encryption
        ConfigReader.use_encryption = self.parse_item('Permissions', 'use_encryption')

        # path to file with password for encryption
        ConfigReader.encryption_password_path = self.parse_item('Permissions', 'encryption_password_path')

import pwd
