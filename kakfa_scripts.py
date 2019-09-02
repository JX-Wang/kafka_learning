import os
import time

def hack():
    print "Stopping kafka service..."
    os.system("bin/kafka-server-stop")
    print "Changing kafka "
    os.system("echo")

if __name__ == '__main__':
    hack()