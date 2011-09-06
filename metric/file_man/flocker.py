"""
Author: Evan Fosmark
URL: http://www.evanfosmark.com/2009/01/cross-platform-file-locking-support-in-python/

This is class for locking file, used for checking generator multiple instances run.

Original code modifications:
30:--    self.lockfile = os.path.join(os.getcwd(), "%s.lock" % file_name)
30:++    self.lockfile = "%s.lock" % file_name
54:++                    #raise 
54:++                    return False
57:++                    #raise FileLockException("Timeout occured.")
57:++                    return False

"""

import os
import time
import errno
from jfiles import JChartFile
 
class FileLockException(Exception):
    pass
 
class FileLock(object):
    """ A file locking mechanism that has context-manager support so 
        you can use it in a with statement. This should be relatively cross
        compatible as it doesn't rely on msvcrt or fcntl for the locking.
    """
 
    def __init__(self, file_name, timeout=10, delay=.05):
        """ Prepare the file locker. Specify the file to lock and optionally
            the maximum timeout and the delay between each attempt to lock.
        """
        self.is_locked = False
        self.lockfile = "%s.lock" % file_name
        self.file_name = file_name
        self.timeout = timeout
        self.delay = delay
        self.jfile = JChartFile()
 
 
    def acquire(self):
        """ Acquire the lock, if possible. If the lock is in use, it check again
            every `wait` seconds. It does this until it either gets the lock or
            exceeds `timeout` number of seconds, in which case it throws 
            an exception.
        """
        start_time = time.time()
        while True:
            try:
                self.fd = os.open(self.lockfile, os.O_CREAT|os.O_EXCL|os.O_RDWR)
                self.jfile.change_perm(self.lockfile)
                break
            except OSError, e:
                if e.errno != errno.EEXIST:
                    #raise 
                    return False
                try:
                    # check file creation/modification time
                    last_modified_time = os.path.getmtime(self.lockfile)
                    if (time.time() - last_modified_time) > 60 * 60 * 2:
                        # if file was created more than an two hours ago lets remove it. possibly it was not deleted
                        os.unlink(self.lockfile)
                        continue
                except OSError:
                    return False
                if (time.time() - start_time) >= self.timeout:
                    #raise FileLockException("Timeout occured.")
                    return False
                time.sleep(self.delay)
        self.is_locked = True
        return True
 
    def release(self):
        """ Get rid of the lock by deleting the lockfile. 
            When working in a `with` statement, this gets automatically 
            called at the end.
        """
        if self.is_locked:
            try:
                os.close(self.fd)
                os.unlink(self.lockfile)
            except OSError:
                pass    
            self.is_locked = False
 
 
    def __enter__(self):
        """ Activated when used in the with statement. 
            Should automatically acquire a lock to be used in the with block.
        """
        if not self.is_locked:
            self.acquire()
        return self
 
 
    def __exit__(self, type, value, traceback):
        """ Activated at the end of the with statement.
            It automatically releases the lock if it isn't locked.
        """
        if self.is_locked:
            self.release()
 
 
    def __del__(self):
        """ Make sure that the FileLock instance doesn't leave a lockfile
            lying around.
        """
        self.release()