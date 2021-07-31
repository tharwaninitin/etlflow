lazy val scala212 = "2.12.14"
lazy val scala213 = "2.13.6"
lazy val scala3   = "3.0.1"
lazy val scala2Versions = List(scala212 ,scala213)
lazy val allScalaVersions = List(scala212, scala213, scala3)

import Dependencies._

lazy val commonSettings = Seq(
  organization := "com.github.tharwaninitin",
  scalacOptions ++= {
    CrossVersion.partialVersion(scalaVersion.value) match {
      case Some((2, 12)) => Seq("-Ypartial-unification")
      case _ => Seq()
    }
  },
  Test / parallelExecution := false,
  libraryDependencies ++= (CrossVersion.partialVersion(scalaVersion.value) match {
    case Some((2, 12)) =>
      Seq(compilerPlugin(("org.typelevel" %% "kind-projector" % "0.13.0").cross(CrossVersion.full)),
          compilerPlugin(("org.scalamacros" % "paradise"  % "2.1.1").cross(CrossVersion.full)),
          "org.scala-lang" % "scala-reflect" % scala212
         )
    case _            => Seq("org.scala-lang" % "scala-reflect" % scala213)
  }),
    testFrameworks += new TestFramework("zio.test.sbt.ZTestFramework")
)

lazy val coreSettings = Seq(
  name := "etlflow-core",
  crossScalaVersions := scala2Versions,
  libraryDependencies ++= coreLibs ++ coreTestLibs
)

lazy val sparkSettings = Seq(
  name := "etlflow-spark",
  crossScalaVersions := List(scala212),
  libraryDependencies ++= sparkLibs ++ coreTestLibs ++ sparkTestLibs,
)

lazy val cloudSettings = Seq(
  name := "etlflow-cloud",
  crossScalaVersions := scala2Versions,
  libraryDependencies ++= cloudLibs ++ coreTestLibs ++ cloudTestLibs,
)

lazy val serverSettings = Seq(
  name := "etlflow-server",
  crossScalaVersions := scala2Versions,
  coverageExcludedPackages := ".*ServerApp;.*HttpServer",
  libraryDependencies ++= serverLibs ++ coreTestLibs,
)

lazy val dbSettings = Seq(
  name := "etlflow-db",
  crossScalaVersions := scala2Versions,
  libraryDependencies ++=  dbLibs ++ dbTestLibs,
)

lazy val utilsSettings = Seq(
  name := "etlflow-utils",
  crossScalaVersions := allScalaVersions,
  libraryDependencies ++=  utilsLibs ++ utilsTestLibs,
)

lazy val httpSettings = Seq(
  name := "etlflow-http",
  crossScalaVersions := scala2Versions,
  libraryDependencies ++= httpLibs
)

lazy val redisSettings = Seq(
  name := "etlflow-redis",
  crossScalaVersions := scala2Versions,
  libraryDependencies ++= redisLibs
)

lazy val jsonSettings = Seq(
  name := "etlflow-json",
  crossScalaVersions :=  allScalaVersions,
  libraryDependencies ++= jsonLibs ++ jsonTestLibs
)

lazy val root = (project in file("."))
  .settings(
    crossScalaVersions := Nil, // crossScalaVersions must be set to Nil on the aggregating project
    publish / skip := true
  )
  .aggregate(utils,db,json,core,spark,cloud,server,http,redis)

lazy val utils = (project in file("modules/utils"))
  .settings(commonSettings)
  .settings(utilsSettings)

lazy val json = (project in file("modules/json"))
  .settings(commonSettings)
  .settings(jsonSettings)
  .dependsOn(utils)

lazy val db = (project in file("modules/db"))
  .settings(commonSettings)
  .settings(dbSettings)
  .dependsOn(utils)

lazy val core = (project in file("modules/core"))
  .settings(commonSettings)
  .settings(coreSettings)
  .enablePlugins(BuildInfoPlugin)
  .settings(
    buildInfoKeys := Seq[BuildInfoKey](
      resolvers,
      Compile / libraryDependencies,
      name, version, scalaVersion, sbtVersion
    ),
    buildInfoOptions += BuildInfoOption.BuildTime,
    buildInfoPackage := "etlflow"
  )
  .dependsOn(db, utils, json)

lazy val cloud = (project in file("modules/cloud"))
  .settings(commonSettings)
  .settings(cloudSettings)
  .dependsOn(core % "compile->compile;test->test")

lazy val server = (project in file("modules/server"))
  .settings(commonSettings)
  .settings(serverSettings)
  .dependsOn(core % "compile->compile;test->test", cloud)

lazy val spark = (project in file("modules/spark"))
  .settings(commonSettings)
  .settings(sparkSettings)
  .dependsOn(core % "compile->compile;test->test")

lazy val http = (project in file("modules/http"))
  .settings(commonSettings)
  .settings(httpSettings)
  .dependsOn(core % "compile->compile;test->test")

lazy val redis = (project in file("modules/redis"))
  .settings(commonSettings)
  .settings(redisSettings)
  .dependsOn(core % "compile->compile;test->test")



