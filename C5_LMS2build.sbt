name := "course5assigment2"

version := "0.1"

scalaVersion := "2.11.8"

val hadoopVersion = "2.7.7"

libraryDependencies += "org.apache.hadoop" % "hadoop-common" % hadoopVersion

libraryDependencies += "org.apache.hadoop" % "hadoop-hdfs" % hadoopVersion

val sparkVersion = "2.3.3"

libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion