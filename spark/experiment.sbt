name := "Experimental Track"

version := "1.0"

scalaVersion := "2.10.4"

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.5.1"

libraryDependencies += "org.apache.spark" %% "spark-graphx" % "1.5.1"

libraryDependencies += "org.apache.spark" %% "spark-mllib" % "1.5.1"

libraryDependencies += "org.apache.spark" % "spark-streaming_2.10" % "1.5.1"

libraryDependencies += "org.apache.spark" % "spark-streaming-kafka_2.10" % "1.5.1"

libraryDependencies += "com.google.guava" % "guava" % "18.0"

libraryDependencies += "org.apache.spark" %% "spark-streaming-kafka" % "1.3.0"

resolvers += "Akka Repository" at "http://repo.akka.io/releases/"
