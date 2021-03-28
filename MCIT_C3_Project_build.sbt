name := "course3project"

version := "0.1"

scalaVersion := "2.11.8"

val hadoopVersion = "2.7.7" /*in the video was 2.6.0*/

libraryDependencies += "org.apache.hadoop" % "hadoop-common" % hadoopVersion

libraryDependencies += "org.apache.hadoop" % "hadoop-hdfs" % hadoopVersion

