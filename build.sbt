name := "linecount-spark-scala"

version := "1.0"

scalaVersion := "2.10.4"

scalacOptions ++= Seq("-deprecation", "-feature", "-unchecked")

mainClass in assembly := Some("edu.luc.cs.LineCount")

resolvers ++= Seq(
  "gkthiruvathukal@bintray" at "http://dl.bintray.com/gkthiruvathukal/maven",
  "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots"
)

libraryDependencies ++= Seq(
  "edu.luc.cs" %% "blockperf" % "0.4.3",
  "com.novocode" % "junit-interface" % "latest.release" % "test",
  "org.apache.spark" %% "spark-core" % "1.4.1" % "provided",
  "com.github.scopt" %% "scopt" % "3.3.0",
  "org.scalatest" %% "scalatest" % "2.2.6" % Test
)
