#!/bin/bash

export HADOOP_CLASSPATH=$(/usr/local/hadoop/bin/hadoop classpath)


mkdir Proj1
javac -classpath ${HADOOP_CLASSPATH} -d Proj1/ Proj1.java
jar -cvf Proj1.jar -C Proj1/ .
/usr/local/hadoop/bin/hadoop jar /usr/local/hadoop/share/hadoop/mapreduce/Proj1/Proj1.jar org.apache.hadoop.ramapo.Proj1 ~/input/utr.txt.csv ~/likedByUser

mkdir userFriends_saved
javac -classpath ${HADOOP_CLASSPATH} -d Proj1/ userFriends_saved.java
jar -cvf userFriends_saved.jar -C Proj1/ .
/usr/local/hadoop/bin/hadoop jar /usr/local/hadoop/share/hadoop/mapreduce/Proj1/userFriends_saved.jar org.apache.hadoop.ramapo.userFriends_saved ~/likedByUser/part-r-00000 ~/userFriends

cp -r ~/likedByUser /usr/local/hadoop/share/hadoop/mapreduce/Proj1/
mkdir finalStep
javac -classpath ${HADOOP_CLASSPATH} -d Proj1/ finalStep.java
jar -cvf finalStep.jar -C Proj1/ .
/usr/local/hadoop/bin/hadoop jar /usr/local/hadoop/share/hadoop/mapreduce/Proj1/finalStep.jar org.apache.hadoop.ramapo.finalStep ~/userFriends/part-r-00000 ~/recommendedList
