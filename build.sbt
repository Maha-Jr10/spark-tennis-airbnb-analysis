name := "spark-project-Intellij"
version := "1.0"
scalaVersion := "2.13.12"

// Spark dependencies (provided by your Spark installation)
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.5.8",
  "org.apache.spark" %% "spark-sql" % "3.5.8",
  "org.apache.spark" %% "spark-graphx" % "3.5.8",
  "org.apache.spark" %% "spark-mllib" % "3.5.8"
)

// Unmanaged GraphFrames JAR (place in lib/)
unmanagedJars in Compile += file("lib/graphframes-0.8.3-spark3.4-s_2.13.jar")