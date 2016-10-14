name := "Big Data Benchmark"

version := "1.0"

scalaVersion := "2.11.8"

//logLevel := Level.Debug

resolvers := Resolver.mavenLocal +: resolvers.value

//unmanagedBase := file("/home/nchaimov/spark/assembly/target/scala-2.10")

//libraryDependencies += "org.apache.spark" %% "spark-launcher" % "1.5.0-SNAPSHOT" from "file:///home/nchaimov/spark/launcher/target/scala-2.10/spark-launcher_2.10-1.5.0-SNAPSHOT.jar"
//libraryDependencies += "org.apache.spark" %% "spark-unsafe" % "1.5.0-SNAPSHOT" from "file:///home/nchaimov/spark/unsafe/target/scala-2.10/spark-unsafe_2.10-1.5.0-SNAPSHOT.jar"
libraryDependencies += "org.apache.spark" %% "spark-core" % "2.0.2-SNAPSHOT"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.0.2-SNAPSHOT"
