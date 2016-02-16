name := "linecount-spark-scala"

version := "1.0"

scalaVersion := "2.10.4"

mainClass in assembly := Some("edu.luc.cs.LineCount")

libraryDependencies ++= Seq(
  "com.novocode" % "junit-interface" % "latest.release" % "test",
  "org.apache.spark" %% "spark-core" % "1.4.1" % "provided",
  "com.github.scopt" %% "scopt" % "3.3.0"
)
