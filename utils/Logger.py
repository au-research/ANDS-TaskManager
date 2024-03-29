# harvester Logger class
# very primitive logging functionality
# for ARDC registry
# Author: u4187959
# created 12/05/2014
#

from datetime import datetime
import os
import myconfig


class Logger:
    __fileName = False
    __file = False
    __current_log_time = False
    logLevels = {'ERROR': 100, 'INFO': 50, 'DEBUG': 10}
    __logLevel = 100

    def __init__(self):
        self.__current_log_time = datetime.now().strftime("%Y-%m-%d")
        self.__fileName = myconfig.log_dir + self.__current_log_time + ".log"
        self.__logLevel = self.logLevels[myconfig.log_level]
        if not os.path.exists(myconfig.log_dir):
            os.makedirs(myconfig.log_dir)
            os.chmod(myconfig.log_dir, 0o775)
        self.log_message("loglevel set to %s:%s" % (str(self.__logLevel), myconfig.log_level), myconfig.log_level)

    def log_message(self, message, logLevel='DEBUG'):
        """
        append to logs if the loglevel is greater or equal to the loglevel that is set by the config
        :param message:
        :type message:
        :param logLevel:
        :type logLevel:
        """
        try:
            if self.logLevels[logLevel] >= self.__logLevel:
                self.rotate_log_file()
                self.__file = open(self.__fileName, "a", 0o777)
                self.__file.write(logLevel + ": " + message + " %s" % datetime.now() + "\n")
                self.__file.close()
        except Exception:
            pass  # bad luck

    def rotate_log_file(self):
        """
        log files are created for daily logs
        """
        if self.__current_log_time != datetime.now().strftime("%Y-%m-%d"):
            self.__current_log_time = datetime.now().strftime("%Y-%m-%d")
            self.__fileName = myconfig.log_dir + self.__current_log_time + ".log"
            number_to_keep = 14
            if len(os.listdir(myconfig.log_dir)) > number_to_keep:
                the_files = self.listdir_fullpath(myconfig.log_dir)
                the_files.sort(key=os.path.getmtime, reverse=True)
                for i in range(number_to_keep, len(the_files)):
                    try:
                        if os.path.isfile(the_files[i]):
                            os.unlink(the_files[i])
                        else:
                            self.delete_directory(the_files[i])
                            os.rmdir(the_files[i])
                    except Exception as e:
                        print(e)

    @staticmethod
    def listdir_fullpath(d):
        return [os.path.join(d, f) for f in os.listdir(d)]

    def delete_directory(self, directory):
        for the_file in os.listdir(directory):
            file_path = os.path.join(directory, the_file)
            try:
                if os.path.isfile(file_path):
                    os.unlink(file_path)
                else:
                    self.delete_directory(file_path)
                    os.rmdir(file_path)
            except Exception as e:
                self.log_message(e, "ERROR")
