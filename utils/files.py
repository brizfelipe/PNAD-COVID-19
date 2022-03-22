from base64 import encode
from email import header
from operator import index
import os 
import sys
import csv
import pandas as pd 
from datetime import datetime


class path:
    
    diretory = os.getcwd()
    pathDBInput = os.path.join(diretory,'pnadFiles','files')
    pathDocumentation = os.path.join(diretory,'pnadFiles','documentacao')
    pathIntermediaria = os.path.join(diretory,'pnadFiles','intermediaria')
    

class Manipulation:

    def readCSV(file,sep):
        maxInt = sys.maxsize
        while True:
            try:
                csv.field_size_limit(maxInt)
                break
            except OverflowError:
                maxInt = int(maxInt/10)

        results = []

        with open(file, encoding="utf-8") as inFile:
            reader = csv.reader(inFile,delimiter=sep)
            data = [row[0] for row in reader]
            results.append(data)

            print('Arquivo' + file + ' baixado')

            return results[0]


    def saveCSVDataFrame(df,path,sep):
        df.to_csv(path_or_buf=path,sep=sep,header=False,index=False)
        print(f'df saved with success! timer: {datetime.now()}')