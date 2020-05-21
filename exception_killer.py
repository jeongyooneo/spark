import time
import os.path
import os
from os import path
from subprocess import Popen, PIPE


def follow(thefile):
    '''generator function that yields new lines in a file
    '''
    # seek the end of the file
    thefile.seek(0, os.SEEK_END)
    
    # start infinite loop
    while not path.exists("completed.txt"):
        # read last line of file
        line = thefile.readline()
        # sleep if file hasn't been updated
        if not line:
            time.sleep(0.1)
            continue

        if "Exception" in line:
            print("Exception happend!! kill")
            p = Popen(["spark_killer.sh"], stdout=PIPE)
            output, err = p.communicate()
            

if __name__ == '__main__':
    
    while not path.exists("spark_log.txt"):
        time.sleep(1)

    logfile = open("spark_log.txt","r")
    follow(logfile)
