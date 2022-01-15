import Dependencies._
import Versions._
import ScalaCompileOptions._

ThisBuild / version := EtlFlowVersion

lazy val commonSettings = Seq(
  scalaVersion := scala212,
  organization := "com.github.tharwaninitin",
  scalacOptions ++= {
    CrossVersion.partialVersion(scalaVersion.value) match {
      case Some((2, _)) => s2copts
      case Some((3, _)) => s3copts
      case _            => Seq()
    }
  },
  Test / parallelExecution := false,
  libraryDependencies ++= Seq("org.scala-lang.modules" %% "scala-collection-compat" % "2.6.0") ++
    (CrossVersion.partialVersion(scalaVersion.value) match {
      case Some((2, 12)) =>
        Seq(
          compilerPlugin(("org.typelevel"  %% "kind-projector" % "0.13.2").cross(CrossVersion.full)),
          compilerPlugin(("org.scalamacros" % "paradise"       % "2.1.1").cross(CrossVersion.full))
        )
      case Some((2, 13)) => Seq()
      case Some((3, 0))  => Seq()
      case _             => Seq()
    }),
  testFrameworks += new TestFramework("zio.test.sbt.ZTestFramework")
)

lazy val coreSettings = Seq(
  name               := "etlflow-core",
  crossScalaVersions := allScalaVersions,
  libraryDependencies ++= coreLibs ++ zioTestLibs ++ coreTestLibs
)

lazy val sparkSettings = Seq(
  name               := "etlflow-spark",
  crossScalaVersions := scala2Versions,
  libraryDependencies ++= sparkLibs ++ zioTestLibs ++ sparkTestLibs
)

lazy val cloudSettings = Seq(
  name               := "etlflow-cloud",
  crossScalaVersions := allScalaVersions,
  libraryDependencies ++= cloudLibs ++ zioTestLibs ++ cloudTestLibs
)

lazy val serverSettings = Seq(
  name                     := "etlflow-server",
  crossScalaVersions       := scala2Versions,
  coverageExcludedPackages := ".*ServerApp;.*HttpServer",
  libraryDependencies ++= serverLibs ++ zioTestLibs ++ coreTestLibs ++ dbTestLibs
)

lazy val dbSettings = Seq(
  name               := "etlflow-db",
  crossScalaVersions := allScalaVersions,
  libraryDependencies ++= dbLibs ++ zioTestLibs ++ coreTestLibs ++ dbTestLibs
)

lazy val httpSettings = Seq(
  name               := "etlflow-http",
  crossScalaVersions := allScalaVersions,
  libraryDependencies ++= httpLibs ++ zioTestLibs
)

lazy val redisSettings = Seq(
  name               := "etlflow-redis",
  crossScalaVersions := scala2Versions,
  libraryDependencies ++= redisLibs ++ zioTestLibs
)

lazy val jsonSettings = Seq(
  name               := "etlflow-json",
  crossScalaVersions := allScalaVersions,
  libraryDependencies ++= jsonLibs ++ zioTestLibs
)

lazy val emailSettings = Seq(
  name               := "etlflow-email",
  crossScalaVersions := allScalaVersions,
  libraryDependencies ++= emailLibs ++ zioTestLibs
)

lazy val awsSettings = Seq(
  name               := "etlflow-aws",
  crossScalaVersions := allScalaVersions,
  libraryDependencies ++= awsLibs ++ zioTestLibs ++ cloudTestLibs
)

lazy val gcpSettings = Seq(
  name               := "etlflow-gcp",
  crossScalaVersions := allScalaVersions,
  libraryDependencies ++= gcpLibs ++ zioTestLibs ++ cloudTestLibs
)

lazy val etlflow = (project in file("."))
  .settings(
    crossScalaVersions := Nil, // crossScalaVersions must be set to Nil on the aggregating project
    publish / skip     := true
  )
  .aggregate(core, server, json, spark, cloud, db, http, redis, email, aws, gcp)

lazy val core = (project in file("modules/core"))
  .settings(commonSettings)
  .settings(coreSettings)

lazy val json = (project in file("modules/json"))
  .settings(commonSettings)
  .settings(jsonSettings)

lazy val db = (project in file("modules/db"))
  .settings(commonSettings)
  .settings(dbSettings)
  .dependsOn(core)

lazy val server = (project in file("modules/server"))
  .settings(commonSettings)
  .settings(serverSettings)
  .enablePlugins(BuildInfoPlugin)
  .settings(
    buildInfoKeys := Seq[BuildInfoKey](
      resolvers,
      Compile / libraryDependencies,
      name,
      version,
      scalaVersion,
      sbtVersion
    ),
    buildInfoOptions += BuildInfoOption.BuildTime,
    buildInfoPackage := "etlflow"
  )
  .dependsOn(core, db, json, gcp)

lazy val cloud = (project in file("modules/cloud"))
  .settings(commonSettings)
  .settings(cloudSettings)
  .dependsOn(core % "compile->compile;test->test", gcp, aws)

lazy val spark = (project in file("modules/spark"))
  .settings(commonSettings)
  .settings(sparkSettings)
  .dependsOn(core % "compile->compile;test->test")

lazy val http = (project in file("modules/http"))
  .settings(commonSettings)
  .settings(httpSettings)
  .dependsOn(core % "compile->compile;test->test", json)

lazy val redis = (project in file("modules/redis"))
  .settings(commonSettings)
  .settings(redisSettings)
  .dependsOn(core % "compile->compile;test->test")

lazy val email = (project in file("modules/email"))
  .settings(commonSettings)
  .settings(emailSettings)
  .dependsOn(core % "compile->compile;test->test")

lazy val aws = (project in file("modules/aws"))
  .settings(commonSettings)
  .settings(awsSettings)
  .dependsOn(core % "compile->compile;test->test")

lazy val gcp = (project in file("modules/gcp"))
  .settings(commonSettings)
  .settings(gcpSettings)
  .dependsOn(core % "compile->compile;test->test")
