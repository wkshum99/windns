import os
import time
import sys

__author__ = 'kenneth.shum@bdpint.com'
__date__ = '$Oct 10, 2016 01:34:00 PM$'

def rename(path, newpath):   
    # today
    today = time.strftime('%Y-%m-%d', time.localtime())

    file = path.split('\\')
    file = file[len(file)-1]

    #filename = path+today+".zip"
    try:
        if os.path.isfile(newpath+"\\"+file) != True:
            os.rename(path, newpath+"\\"+file)
        else:
            os.unlink(path)
    except:
        print("Error encountered while processing the file")
        raise
    
if __name__ == "__main__":

    if len(sys.argv) != 3:
        sys.exit("number of input parameters is incorrect")

    path = sys.argv[1]
    newpath = sys.argv[2]   
    
    rename(path, newpath)