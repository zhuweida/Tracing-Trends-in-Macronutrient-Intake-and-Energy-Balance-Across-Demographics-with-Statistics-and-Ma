"""
the function of converting RDD into csv file is based on
http://stackoverflow.com/questions/31898964/how-to-write-the-resulting-rdd-to-a-csv-file-in-spark-python/31899173
"""
import re
import argparse
import collections
import sys
from pyspark import SparkContext,SparkConf
import csv,io
conf = SparkConf()
sc = SparkContext(conf=conf)
global t
t=[]

def list_to_csv_str(x):
    """Given a list of strings, returns a properly-csv-formatted string."""
    output = io.StringIO("")
    csv.writer(output).writerow(x)
    return output.getvalue().strip() # remove extra newline
def float_number(x):
	TEMP=[]
	a=0   #to record whether there are missing data in the row
	for i in range(len(x)):
		if x[i] == '':                #we use a to record whether there is missing value in this row  
			a=a+1
		if x[i] == '5.40E-79':
			x[i]=0
	if x[11]=='7' or x[11]=='9':            #control the missing education level
		a=a+1
			
	if a == 0:                        #if a is still 0 it means there are no missing value in this column
		#if x[1] == '1':		#if the column of recall is 1
		for i in range(len(x)):
			TEMP.append(float(x[i]))  #put all of x which we are satisfied with into list of which name is TEMP
	return TEMP
	
def repreprocessing(x):
	if x[4]< 1228.87:                       #calorie
		x[4]=1
	if x[4]>1228.87 and x[4]<2330.9783 or x[4]==1228.87:
		x[4]=2
	if x[4]>4018.8066 or x[4]==4018.8066:
		x[4]=4
	if x[4]>2330.9783 and x[4]<4018.8066 or x[4]==2330.9783:
		x[4]=3
		
		
	if x[5]<46.139 or x[5]==46.139:  #for protein
		x[5]=1
	if x[5]>46.139 and x[5]<98.715 or x[5]==98.715:
		x[5]=2
	if x[5]>98.715 and x[5]<190.8605:
		x[5]=3
	if x[5]>190.8605:
		x[5]=4
	
	if x[6]<146.28:    #for carbon
		x[6]=1
	if x[6]>146.28 and x[6]<283.238:
		x[6]=2
	if x[6]>283.238 and x[6]<490.6711:
		x[6]=3
	if x[6]>490.6711:
		x[6]=4
	
	
	if x[7]<43.5066: #for fat
		x[7]=1
	if x[7]>43.5066 and x[7]<95.1719:
		x[7]=2
	if x[7]>95.1719 and x[7]<178.4745:
		x[7]=3
	if x[7]>178.4745:
		x[7]=4
	
	if x[9]>20 or x[9]==20 and x[9]<30: #for age
		x[9]=1
	if x[9]>30 and x[9]<45 or x[9]==45:
		x[9]=2
	if x[9]>45 and x[9]<60 or x[9]==60:
		x[9]=3
	if x[9]>60 and x[9]<80 or x[9]==80:
		x[9]=4
	
	if x[11]==1:#education
		x[11]=1
	if x[11]==2 or x[11]==3:
		x[11]=2
	if x[11]==4 or x[11]==5:
		x[11]=3
	
	if x[12]<1.29 or x[12]==1.29:#poverty level
		x[12]=1
	if x[12]>1.29 and x[12]<4.33:
		x[12]=2
	if x[12]>4.33:
		x[12]=3
	
	
	
	
	
 


	
	return x
def toCSVLine(data):
	return ','.join(str(d) for d in data)
def convert(x):
	t.append(x[1])
	return t
def processFile(fileName):
	t=[]
	line=sc.textFile(fileName)
	a=line.zipWithIndex()
	b=a.filter(lambda x:x[1]>0).map(lambda x:x[0].split(','))         #make sure to remove the first row which is the name of column(string)
	c=b.map(lambda x:[str(y) for y in x]).map(lambda x:(float_number(x)))
	
	c9=c.filter(lambda x:len(x)>0)
	preprocessing=c9.map(lambda x:(repreprocessing(x)))
	d=c9.map(lambda x:convert(x))
	t=d.collect()[-1]
	lines=preprocessing.map(toCSVLine)
	lines.saveAsTextFile("cluster2.csv")

def quiet_logs(sc):
	logger = sc._jvm.org.apache.log4j
	logger.LogManager.getLogger("org"). setLevel( logger.Level.ERROR )
	logger.LogManager.getLogger("akka").setLevel( logger.Level.ERROR )

def main():
	quiet_logs(sc)
	parser = argparse.ArgumentParser()
	parser.add_argument('filename', help="filename of input text", nargs="+")
	args = parser.parse_args()

	for filename in args.filename:
		processFile (sys.argv[1])
	

	
		
if __name__ == "__main__":
	main()


	
