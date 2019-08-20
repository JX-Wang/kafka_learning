# encoding : utf-8
"""
Integration result files
========================
Author @ Wangjunxiong
Date @ 2019.8.13
"""

import os
import schedule
import time
ORIGINDICT = 'SOURCE/'
RSTDICT = "RST/"
TIMEOUT = 1200
FREQUENT = 30  # seconds


class IntergrationResultFiles:
    def __init__(self):
        pass

    def monitor(self):
        schedule.every(FREQUENT).seconds.do(self.do)
        while True:
            schedule.run_pending()
            time.sleep(1)
        pass

    def gettotalnum(self, filename):
        """
        TODO: resolve total number
        :param filename -> str
        :return: total num -> int
        """
        filename = str(filename)
        if not filename:
            return "Error Empty File Name"
        else:
            pass
        try:
            filename_mirror = filename[::-1]
            file_without_md5 = filename_mirror[filename_mirror.index("_") + 1:]
            total = file_without_md5[:file_without_md5.index("_")][::-1]
            return int(total)  # return total num
        except Exception as e:
            return "Fesolve File Name Error", str(e)

    def addfiles(self, dirname):
        """
        :param dirname: second floor dir name
        :return: bool:
        """
        addfilename = RSTDICT + dirname  # Result dict
        datafileslist = os.listdir(ORIGINDICT+dirname)  # source dict

        try:
            f = open(addfilename, "w")
            tmp_rst = []
            for datafile in datafileslist:
                try:
                    f_tmp = open(ORIGINDICT+dirname+"/"+datafile, 'r')
                    data = f_tmp.read()
                    tmp_rst.append(data)
                except Exception as e:
                    raise str(e)
            for d in tmp_rst:
                f.write(d)

            os.system(":> "+ORIGINDICT+" %s/Done" % dirname)  # mark this file, mean this dict has been done
        except Exception as e:
            return "AddFile Error", str(e)

    def do(self):
        # print os.getcwd()
        msg = os.listdir(ORIGINDICT)
        for dirname in msg:
            dict_name = ORIGINDICT + dirname
            rst = os.listdir(dict_name)

            if "Done" in rst:  # means this SOURCE has been finished
                continue

            total_num = self.gettotalnum(rst[0])  # get the total files num

            if total_num == len(rst):  # if this task done
                self.addfiles(dirname)  #

            else:
                # open this SOURCE and check time !
                filelist = os.listdir(dict_name)
                filelist.sort(key=lambda x: int(x[:10]))
                final_filename = filelist[0]
                timestart = os.path.getctime(final_filename)  # get file create time / not fit for windows_sys
                print timestart
                timenow = time.time()
                print timenow
                if timenow - timestart >= 1200:
                    self.addfiles(dict_name)
                else:
                    pass
        print "Done"


if __name__ == '__main__':
    I = IntergrationResultFiles()
    # I.monitor()
    I.do()
    # print I.gettotalnum("1566302883_id_01_3_md5")
