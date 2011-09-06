import logging
import logging.handlers
import os, tempfile
from datetime import date

class MLogger:
    def __init__(self, name):
        self._logger = logging.getLogger(name)
        self._logger.setLevel(logging.INFO)
        log_name = 'log-%s.txt' % date.today()

        full_log_dir = os.path.join(os.path.split(os.path.split(os.path.split(os.path.abspath( __file__ ))[0])[0])[0], 'log')
        if not os.path.isdir(full_log_dir):
            try:
                os.makedirs(full_log_dir)
            except OSError, e:
                raise Exception("cannot create dir %s. %s" % (full_log_dir, e))
            try:
                os.chmod(full_log_dir, 0777)
            except OSError, e:
                pass
        else:
            try:
                os.chmod(full_log_dir, 0777)
            except OSError, e:
                pass
        full_log_name = os.path.join(full_log_dir, log_name)
         
        try:
                os.chmod(full_log_name, 0777)
        except OSError:
            pass
        try:
            self._ch = logging.FileHandler(full_log_name)
        except IOError:
            tmp = tempfile.mkstemp(prefix='log_', dir = full_log_dir)
            self._ch = logging.FileHandler(tmp[1])
        
        self._formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s","%Y-%m-%d %H:%M:%S")
        self._ch.setFormatter(self._formatter)
        self._logger.addHandler(self._ch)
    def get_logger(self):
        return self._logger
