val scala3Version = "3.7.4"
val hadoopVersion = "3.4.1"
//val sparkVersion = "3.5.7"

lazy val root = project
  .in(file("."))
  .settings(
    name := "App",
    version := "1.0.0",
    assembly / mainClass := some("App"),
    assembly / assemblyJarName := "app.jar",
    ThisBuild / assemblyMergeStrategy := {
      case x => MergeStrategy.first
    },
    scalaVersion := scala3Version,
    scalacOptions ++= Seq("-unchecked", "-deprecation"),
    scalacOptions ++= Seq("-java-output-version", "8"),
    javacOptions ++= Seq("-source", "1.8", "-target", "1.8"),

    libraryDependencies ++= Seq(
      "com.lihaoyi" %% "upickle" % "4.4.1",
      "org.apache.hadoop" % "hadoop-mapreduce-client-core" % hadoopVersion % "provided",
      "org.apache.hadoop" % "hadoop-mapreduce-client-jobclient" % hadoopVersion % "provided",
      "org.apache.hadoop" % "hadoop-mapreduce-client-app" % hadoopVersion % "provided",
      "org.apache.hadoop" % "hadoop-client-api" % hadoopVersion % "provided",
      "org.apache.hadoop" % "hadoop-yarn-client" % hadoopVersion % "provided",
      "org.apache.hadoop" % "hadoop-hdfs-client" % hadoopVersion % "provided",
      //("org.apache.spark" %% "spark-core" % sparkVersion).cross(CrossVersion.for3Use2_13),
      //("org.apache.spark" %% "spark-streaming" % sparkVersion).cross(CrossVersion.for3Use2_13),
    ),
  )
