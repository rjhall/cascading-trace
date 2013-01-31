import AssemblyKeys._ // put this at the top of the file

assemblySettings

name := "cascading-trace"

version := "0.0.1-SNAPSHOT"

organization := "com.etsy"

resolvers += "Concurrent Maven Repo" at "http://conjars.org/repo"

libraryDependencies += "cascading" % "cascading-hadoop" % "2.0.0"

libraryDependencies += "com.twitter" % "scalding_2.9.1" % "0.8.2-SNAPSHOT"

libraryDependencies += "com.twitter" % "algebird_2.9.1" % "0.1.7-SNAPSHOT"

libraryDependencies += "org.scala-tools.testing" % "specs_2.8.1" % "1.6.6" % "test"

parallelExecution in Test := false
