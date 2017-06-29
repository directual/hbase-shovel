name := "hbase-shovel"

version := "1.0"

scalaVersion := "2.12.1"

resolvers ++= Seq(
  "Hadoop Releases" at "https://repository.cloudera.com/content/repositories/releases/"
)

libraryDependencies ++= Seq(
  "com.google.guava" % "guava" % "15.0",
  "org.apache.hadoop" % "hadoop-common" % "2.6.0",
  "org.apache.hadoop" % "hadoop-mapred" % "0.22.0",
  "org.apache.hbase" % "hbase-common" % "1.0.0",
  "org.apache.hbase" % "hbase-client" % "1.0.0",
  "com.github.scopt" %% "scopt" % "3.6.0"
)

lazy val root = (project in file(".")).
  settings(
    name := "shovel"
  )