"""
the function of converting RDD into csv file is based on
http://stackoverflow.com/questions/31898964/how-to-write-the-resulting-rdd-to-a-csv-file-in-spark-python/31899173
And some of the initialization code is provided by our instructor Dr. Taufer.
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
column=[]
for i in range(20):
	t.append([])

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
			
	if a == 0:                        #if a is still 0 it means there are no missing value in this column
		#if x[1] == '1':		#if the column of recall is 1
		for i in range(len(x)):
			TEMP.append(float(x[i]))  #put all of x which we are satisfied with into list of which name is TEMP
	
	
	return TEMP
def toCSVLine(data):
	return ','.join(str(d) for d in data)
def convert(x):
	for i in range(len(x)):
		t[i].append(x[i])
	return t
def processFile(fileName):
	t=[]
	line=sc.textFile(fileName)
	a=line.zipWithIndex()
	b=a.filter(lambda x:x[1]>0).map(lambda x:x[0].split(','))         #make sure to remove the first row which is the name of column(string)
	c=b.map(lambda x:[str(y) for y in x]).map(lambda x:(float_number(x)))
	data=c.filter(lambda x:len(x)>0)         #row_data
	d=data.map(lambda x:convert(x))
	
	t=d.collect()
	for i in range(8):
		column.append(t[-1][i])
	colrdd=sc.parallelize(column)            #col data
	calorie=sc.parallelize(column[4])
	protein=sc.parallelize(column[5])
	carbon=sc.parallelize(column[6])
	fat=sc.parallelize(column[7])
	
	
	calorie_mean=calorie.mean()
	protein_mean=protein.mean()
	carbon_mean=carbon.mean()
	fat_mean=fat.mean()
	
	calorie_sampleStdev=calorie.sampleStdev()
	protein_sampleStdev=protein.sampleStdev()
	carbon_sampleStdev=carbon.sampleStdev()
	fat_sampleStdev=fat.sampleStdev()
	
	calorie_variance=calorie.sampleVariance()
	protein_variance=protein.sampleVariance()
	carbon_variance=carbon.sampleVariance()
	fat_variance=fat.sampleVariance()
	
	cor_protein_energy = protein.zip(calorie)
	cor_carbon_energy=carbon.zip(calorie)
	cor_fat_energy=fat.zip(calorie)
	
	
	cor1=cor_protein_energy.map(lambda x:((x[0]-protein_mean)*(x[1]-calorie_mean)/(protein_sampleStdev*calorie_sampleStdev)))
	cor2=cor_carbon_energy.map(lambda x:((x[0]-protein_mean)*(x[1]-calorie_mean)/(carbon_sampleStdev*calorie_sampleStdev)))
	cor3=cor_fat_energy.map(lambda x:((x[0]-protein_mean)*(x[1]-calorie_mean)/(fat_sampleStdev*calorie_sampleStdev)))
	print cor1.collect()
	number = cor1.count()
	
	cor=cor1.zip(cor2).zip(cor3)
	print "protein_mean:"+str(protein_mean)
	print "carbon_mean:"+str(carbon_mean)
	print "fat_mean:"+str(fat_mean)
	print "calorie_mean"+str(calorie_mean)
	print "protein_variance:"+str(protein_variance)
	print "carbon_variance:"+str(carbon_variance)
	print "fat_variance:"+str(fat_variance)
	print "calorie_variance:"+str(calorie_variance)
	print "number of samples:"+str(number)
	print "mean correlation of protein and calorie:" +str(cor1.mean())
	print "mean correlation of carbon and calorie:" +str(cor2.mean())
	print "mean correlation of fat and calorie:"+str(cor3.mean())
	
	
	print "remind: everytime you run the code you should change the csv name in line 104 of the code to get the csv of correlation "
	cors=cor.map(toCSVLine)
	cors.saveAsTextFile("2010_correlation_dietarydata.csv")
	
	
	
	
	



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


	
