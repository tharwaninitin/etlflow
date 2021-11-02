import Versions._

lazy val examplespark = (project in file("examplespark"))
  .settings(
    name := "examplespark",
    organization := "com.github.tharwaninitin",
    crossScalaVersions := List(scala212),
    libraryDependencies ++= List(
      "com.github.tharwaninitin" %% "etlflow-spark" % EtlFlowVersion,
      "org.apache.spark" %% "spark-sql" % SparkVersion excludeAll ExclusionRule(organization = "org.scala-lang.modules"),
      "ch.qos.logback" % "logback-classic" % LogbackVersion,
      "com.google.cloud.bigdataoss" % "gcs-connector" % HadoopGCSVersion,
      "com.google.cloud.spark" %% "spark-bigquery-with-dependencies" % SparkBQVersion
    ),
    Compile / mainClass := Some("examples.LoadData"),
    assembly / mainClass := Some("examples.LoadData"),
    assembly / assemblyMergeStrategy := {
      case PathList("META-INF", _*) => MergeStrategy.discard
      case "META-INF/services/org.apache.spark.sql.sources.DataSourceRegister" => MergeStrategy.concat
      case _ => MergeStrategy.first
    },
    assembly / assemblyShadeRules := Seq(
      ShadeRule.rename("com.google.common.**" -> "repackaged.com.google.common.@1").inAll,
      ShadeRule.rename("io.grpc.**" -> "repackaged.io.grpc.@1").inAll,
      ShadeRule.rename("shapeless.**" -> "shadeshapless.@1").inAll
    )
  )