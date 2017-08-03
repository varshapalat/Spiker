name := "Spike"

version := "1.0"

scalaVersion := "2.11.8"

mainClass := Some("SpikeMain")

unmanagedJars in Compile := (baseDirectory.value ** "*.jar").classpath

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.1.0" exclude("org.scala-lang", "scala-reflect") exclude("org.scala-lang", "scala-compiler") exclude("org.scala-lang.modules", "scala-xml" ),
  "org.scala-lang.modules" %% "scala-xml" % "1.0.4",
  "org.apache.spark" %% "spark-sql" % "2.1.0",
  "org.apache.spark" %% "spark-graphx" % "2.1.0"
//  "com.microsoft.sqlserver" % "sqljdbc4" % "4.0" % "test"
)
