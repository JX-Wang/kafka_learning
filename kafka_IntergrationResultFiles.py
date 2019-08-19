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
class IntergrationResultFiles:
    def __init__(self):
        pass

    def monitor(self):
        schedule.every(5).minutes.do(self.do())
        while True:
            schedule.run_pending()
            time.sleep(1)
        pass

    def gettotalnum(self, filename):
        filename = str(filename)
        return filename.index("flag index")  # return total num

    def addfiles(self, dirname):
        """
        :param dirname: second floor dir name
        :return: bool:
        """
        addfilename = RSTDICT + dirname
        datafileslist = os.listdir(ORIGINDICT+dirname)
        with open(addfilename, "w") as f:
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

            os.system(":> "+ORIGINDICT+" %s/Done" % dirname)  # mark this file

            return True
        return False

    def do(self):
        # print os.getcwd()
        msg = os.listdir(ORIGINDICT)
        print msg
        for dirname in msg:
            dict_name = ORIGINDICT + dirname
            rst = os.listdir(dict_name)

            if "Done" in rst:  # means this SOURCE has been finished
                continue

            total_num = self.gettotalnum(rst[0])  # get the total files num

            if total_num == len(rst):  # if this task done
                yield self.addfiles(dirname)  #
                continue

            else:
                # open this SOURCE and check time !
                filelist = os.listdir(dict_name)
                filelist.sort(key=lambda x: int(x))
                final_filename = filelist[0]
                timestart = os.path.getctime(final_filename)  # get file create time
                timenow = time.time()
                if timenow - timestart >= 1200:
                    self.addfiles(dict_name)
                else:
                    pass
        print msg
        pass


if __name__ == '__main__':
    I = IntergrationResultFiles()
    I.do()