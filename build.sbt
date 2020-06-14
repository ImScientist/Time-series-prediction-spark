resolvers += "Spark Packages Repo" at "https://dl.bintray.com/spark-packages/maven"
resolvers += "Something else" at "https://mvnrepository.com/artifact"

name := "m5_scala"

version := "0.2"

organization := "com.github.imscientist"

scalaVersion := "2.11.12"

libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.1" % "test"
libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.4" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.4" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-mllib" % "2.4.4" % "provided"
libraryDependencies += "com.microsoft.ml.spark" %% "mmlspark" % "0.18.1"


libraryDependencies += "MrPowers" % "spark-fast-tests" % "0.21.1-s_2.11" % Test
//libraryDependencies += "mrpowers" % "spark-daria" % "0.37.1-s_2.12"

//libraryDependencies += "org.scalanlp" %% "breeze" % "1.0"
//libraryDependencies += "org.scalanlp" %% "breeze" % "0.13.2"  //I can not import this!


// test suite settings
fork in Test := true
javaOptions ++= Seq("-Xms512M", "-Xmx2048M", "-XX:+CMSClassUnloadingEnabled")
parallelExecution in Test := false

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}
