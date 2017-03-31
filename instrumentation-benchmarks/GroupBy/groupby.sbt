name := "GroupByTest"

version := "1.0"

scalaVersion := "2.11.8"

//logLevel := Level.Debug

resolvers := Resolver.mavenLocal +: resolvers.value

//unmanagedBase := file("/home/nchaimov/spark/assembly/target/scala-2.10")

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.5.2"
//libraryDependencies += "org.apache.spark" %% "spark-graphx" % "1.5.0-SNAPSHOT"

