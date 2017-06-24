"""
The book Zaharia M., et al. Learning Spark (O'Reilly, 2015)(274s) and the code on
https://github.com/apache/spark/blob/master/examples/src/main/python/kmeans.py
helped me better understand the k-means clustering algorithm.
And some of the initialization code is provided by my instructor Dr. Taufer .
"""
import matplotlib
matplotlib.use('Agg')
import re
import argparse
import collections
import sys
import numpy as np
import matplotlib.pyplot as plt

from pyspark import SparkContext,SparkConf
conf = SparkConf()
sc = SparkContext(conf=conf)


def quiet_logs(sc):
	logger = sc._jvm.org.apache.log4j
	logger.LogManager.getLogger("org"). setLevel( logger.Level.ERROR )
	logger.LogManager.getLogger("akka").setLevel( logger.Level.ERROR )

def cloest(point,centers):
	final_index=0
	minimal_distance=float("inf")
	for i in range(len(centers)):
		distance = np.sum((point[0]-centers[i])**2)
		if distance < minimal_distance:
			minimal_distance=distance
			final_index=i
	return final_index
def close(centers,newcenters):
	i=newcenters[0]
	distance=(newcenters[1][0]-centers[i][0])**2+(newcenters[1][1]-centers[i][1])**2
	return distance
	
	
	
	
def floatdata(line):
	return np.array([float(x) for x in line.split(' ')])
def processFile(fileName):
	global number_of_kmean
	global centers
	global points
	number_of_kmean=int(sys.argv[2])  
	endurable_error=0.01
	lines=sc.textFile(fileName)
	data=lines.map(floatdata).cache()
	centers=data.takeSample(False,number_of_kmean,1)
	actual_error=5.5
	j=1
	
	
	while actual_error > endurable_error:
		clustered_points = data.map(lambda point:(cloest(point,centers),(point,1))) 
		clustered_points2 = data.map(lambda point:(cloest(point,centers),point)) 
		points=clustered_points2.collect()
		
		total_distance = clustered_points.reduceByKey(lambda x,y:(x[0]+y[0],x[1]+y[1])) 
		
		newcenter=total_distance.map(lambda x:(x[0],x[1][0]/x[1][1]))
		newcenters=newcenter.collect()
		
		actual_error=sum(np.sum((centers[i]-newcenter)**2) for (i,newcenter) in newcenters)
		print "%dth error is:%d"%(j,actual_error)
		j = j+1
		for (i,newcenter) in newcenters:
			centers[i] = newcenter
		
		
		
	
	
	print "the final centers are:" + str(centers)
	
	
	
	
	
	
	
	

def main():
	quiet_logs(sc)
	parser = argparse.ArgumentParser()
	parser.add_argument('filename', help="filename of input text", nargs="+")
	args = parser.parse_args()

	for filename in args.filename:
		processFile (sys.argv[1])
	x=[]
	y=[]
	x2=[]
	y2=[]
	for i in range(0,number_of_kmean):
		x.append(centers[i][0])
		y.append(centers[i][1])
		
	for j in range(0,len(points)):
		x2.append(points[j][1][0])
		y2.append(points[j][1][1])
	plt.scatter(x2,y2,label='original points',color='r',marker='o',s=20) 
	plt.scatter(x,y,label='centers',color='b',marker='*',s=20)
		
	plt.xlabel('x')
	plt.ylabel('y')
	plt.title('CISC876 k-means project')
	plt.legend()
	plt.show()
	print "list in x2:" + str(x2)
	print "the final centers are:" + str(centers)
	print "points:" + str(points)
	print "points[0]" +str(points[0][1][1])
	print "centers:" +str(centers[0][0])
	plt.savefig('myfigg')
		
if __name__ == "__main__":
	main()


	
