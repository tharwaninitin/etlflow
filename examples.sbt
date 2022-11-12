import ScalaCompileOptions._
import Versions._

lazy val examples = (project in file("examples"))
  .settings(
    crossScalaVersions := Nil, // crossScalaVersions must be set to Nil on the aggregating project
    publish / skip     := true,
    scalacOptions ++= {
      CrossVersion.partialVersion(scalaVersion.value) match {
        case Some((2, 12)) => s2copts ++ s212copts
        case Some((2, 13)) => s2copts
        case Some((3, _))  => s3copts
        case _             => Seq()
      }
    }
  )
  .aggregate(examplecore, examplespark)

lazy val examplecore = (project in file("examples/examplecore"))
  .settings(
    name               := "examplecore",
    crossScalaVersions := AllScalaVersions,
    libraryDependencies ++= List(
      "com.github.tharwaninitin" %% "etlflow-core"    % EtlFlowVersion,
      "com.github.tharwaninitin" %% "etlflow-k8s"     % EtlFlowVersion,
      "com.github.tharwaninitin" %% "etlflow-db"      % EtlFlowVersion,
      "ch.qos.logback"            % "logback-classic" % LogbackVersion,
      "org.postgresql"            % "postgresql"      % PgVersion
    )
  )

lazy val examplespark = (project in file("examples/examplespark"))
  .settings(
    name               := "examplespark",
    crossScalaVersions := Scala2Versions,
    libraryDependencies ++= List(
      "com.github.tharwaninitin" %% "etlflow-spark" % EtlFlowVersion,
      ("org.apache.spark" %% "spark-sql"       % SparkVersion).excludeAll(ExclusionRule(organization = "org.scala-lang.modules")),
      "ch.qos.logback"     % "logback-classic" % LogbackVersion,
      "com.google.cloud.bigdataoss" % "gcs-connector" % HadoopGCSVersion
      // "com.google.cloud.spark" %% "spark-bigquery-with-dependencies" % SparkBQVersion
    ),
    excludeDependencies += "org.slf4j" % "slf4j-log4j12",
    Compile / mainClass               := Some("examples.LoadData"),
    assembly / mainClass              := Some("examples.LoadData"),
    assembly / assemblyMergeStrategy := {
      case PathList("META-INF", _*)                                            => MergeStrategy.discard
      case "META-INF/services/org.apache.spark.sql.sources.DataSourceRegister" => MergeStrategy.concat
      case _                                                                   => MergeStrategy.first
    },
    assembly / assemblyShadeRules := Seq(
      ShadeRule.rename("com.google.common.**" -> "repackaged.com.google.common.@1").inAll,
      ShadeRule.rename("io.grpc.**" -> "repackaged.io.grpc.@1").inAll,
      ShadeRule.rename("shapeless.**" -> "shadeshapless.@1").inAll
    )
  )
