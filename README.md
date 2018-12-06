# BigDataGroupProj

**Basic Details:** 

GC time elapsed (ms)=5577  or 5.577   seconds 

CPU time spent (ms)=180160 or 180.160 seconds 

.

.


**Advanced Details:**

Total time spent by all maps in occupied slots (ms)=42977808 

Total time spent by all reduces in occupied slots (ms)=6356930 

Total time spent by all map tasks (ms)=273744 

Total time spent by all reduce tasks (ms)=40490 

Total vcore-milliseconds taken by all map tasks=273744 

Total vcore-milliseconds taken by all reduce tasks=40490 

Total megabyte-milliseconds taken by all map tasks=1372004928 

Total megabyte-milliseconds taken by all reduce tasks=202935880 

.

**To get the dataset:**

Go here: http://stat-computing.org/dataexpo/2009/the-data.html?fbclid=IwAR32jlV4CbxdxhpKPwJcTUVqwOhPQT1YtOLKialWda-dW0G3_VIlyoS49Lg

Click the link for the '2008' dataset, the csv should begin to download immediately.


**To Run Program:**

1. Login to bridges
	ssh -P 2222 username@bridges.psc.xsede.org
2. Create input directory named "input"
	mkdir input
3. Write WordCount.java into input directory using vim
	vim WordCount.java
4. inport text files into input directory using filezilla
5. Start Hadoop in bridges
	interact -N 4 -t 01:00:00
	module load hadoop
	start-hadoop.sh
6. Compile WordCount.java and create a jar
	hadoop com.sun.tools.javac.Main BookX.java
	jar cf wc.jar BookX*.class
7. Create a directory to store input files into the hdfs
	hadoop fs -mkdir -p inn
8. Load input text file into hdfs
	hadoop fs -put $HOME/csv/2008.csv inn
9. Run program
	hadoop jar wc.jar BookX inn/2008.csv output
*note: different output needed each time program is run
10. Print output
	hadoop fs -cat output/*
