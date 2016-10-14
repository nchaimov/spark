name := "LiveJournal Page Rank"

version := "1.0"

scalaVersion := "2.11.8"

resolvers := Resolver.mavenLocal +: resolvers.value

//unmanagedBase := file("/home/nchaimov/spark/assembly/target/scala-2.10")

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.0.2-SNAPSHOT"
libraryDependencies += "org.apache.spark" %% "spark-graphx" % "2.0.2-SNAPSHOT"
