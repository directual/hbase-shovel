import sbt.Keys.libraryDependencies

name := "hbase-shovel"

version := "1.0"

scalaVersion := "2.12.1"

resolvers ++= Seq(
  "Hadoop Releases" at "https://repository.cloudera.com/content/repositories/releases/",
  "Artima Maven Repository" at "http://repo.artima.com/releases"
)

libraryDependencies ++= Seq(
  "com.google.guava" % "guava" % "15.0",
  "org.apache.hadoop" % "hadoop-common" % "2.6.0",
  "org.apache.hadoop" % "hadoop-mapred" % "0.22.0",
  "org.apache.hbase" % "hbase-common" % "1.0.0",
  "org.apache.hbase" % "hbase-client" % "1.0.0",
  "com.github.scopt" %% "scopt" % "3.6.0",
  "org.scalactic" %% "scalactic" % "3.0.1",
  "org.scalatest" %% "scalatest" % "3.0.1" % "test"
)
lazy val root = (project in file(".")).
  settings(
    name := "shovel"
  )

mergeStrategy in assembly <<= (mergeStrategy in assembly) { (old) =>
  {
    case PathList("javax", "servlet", xs @ _*) => MergeStrategy.last
    case PathList("javax", "activation", xs @ _*) => MergeStrategy.last
    case PathList("org", "apache", xs @ _*) => MergeStrategy.last
    case PathList("com", "google", xs @ _*) => MergeStrategy.last
    case PathList("com", "esotericsoftware", xs @ _*) => MergeStrategy.last
    case PathList("com", "codahale", xs @ _*) => MergeStrategy.last
    case PathList("com", "yammer", xs @ _*) => MergeStrategy.last
    case "about.html" => MergeStrategy.rename
    case "META-INF/ECLIPSEF.RSA" => MergeStrategy.last
    case "META-INF/mailcap" => MergeStrategy.last
    case "META-INF/mimetypes.default" => MergeStrategy.last
    case "plugin.properties" => MergeStrategy.last
    case "log4j.properties" => MergeStrategy.last
    case x => old(x)
  }
}
