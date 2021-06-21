lazy val scala212 = "2.12.13"
lazy val scala213 = "2.13.6"
lazy val supportedScalaVersions = List(scala212)
lazy val sparkSupportedScalaVersions = List(scala212)

import Dependencies._

lazy val commonSettings = Seq(
  organization := "com.github.tharwaninitin",
  excludeDependencies ++= Seq("org.slf4j" % "slf4j-log4j12"),
  scalacOptions ++= Seq("-Ypartial-unification"),
  Test / parallelExecution := false,
  addCompilerPlugin("org.typelevel" %% "kind-projector" % "0.13.0" cross CrossVersion.full),
  addCompilerPlugin("org.scalamacros" % "paradise"  % "2.1.1" cross CrossVersion.full),
  testFrameworks += new TestFramework("zio.test.sbt.ZTestFramework")
)

lazy val coreSettings = Seq(
  name := "etlflow-core",
  crossScalaVersions := supportedScalaVersions,
  libraryDependencies ++= coreLibs ++ coreTestLibs,
  //https://stackoverflow.com/questions/36501352/how-to-force-a-specific-version-of-dependency
  dependencyOverrides ++= {
    Seq(
      "com.fasterxml.jackson.module" % "jackson-module-scala_2.12" % "2.6.7.1",
      "com.fasterxml.jackson.core" % "jackson-databind" % "2.6.7.1",
    )
  }
)

lazy val sparkSettings = Seq(
  name := "etlflow-spark",
  crossScalaVersions := sparkSupportedScalaVersions,
  libraryDependencies ++= sparkLibs ++ coreTestLibs ++ sparkTestLibs,
)

lazy val cloudSettings = Seq(
  name := "etlflow-cloud",
  crossScalaVersions := supportedScalaVersions,
  libraryDependencies ++= cloudLibs ++ coreTestLibs ++ cloudTestLibs,
)

lazy val serverSettings = Seq(
  name := "etlflow-server",
  crossScalaVersions := supportedScalaVersions,
  libraryDependencies ++= serverLibs ++ coreTestLibs,
)

lazy val dbSettings = Seq(
  name := "etlflow-db",
  crossScalaVersions := supportedScalaVersions,
  libraryDependencies ++=  dbLibs ++ coreTestLibs ++ dbTestLibs,
)

lazy val utilsSettings = Seq(
  name := "etlflow-utils",
  crossScalaVersions := supportedScalaVersions
)

lazy val httpSettings = Seq(
  name := "etlflow-http",
  crossScalaVersions := supportedScalaVersions,
  libraryDependencies ++= httpLibs
)

lazy val redisSettings = Seq(
  name := "etlflow-redis",
  crossScalaVersions := supportedScalaVersions,
  libraryDependencies ++= redisLibs
)

lazy val root = (project in file("."))
  .settings(
    crossScalaVersions := Nil, // crossScalaVersions must be set to Nil on the aggregating project
    publish / skip := true
  )
  .aggregate(db,core,spark,cloud,server,utils,http,redis)

lazy val core = (project in file("modules/core"))
  .settings(commonSettings)
  .settings(coreSettings)
  .enablePlugins(BuildInfoPlugin)
  .settings(
    initialCommands := "import etlflow._",
    buildInfoKeys := Seq[BuildInfoKey](
      resolvers,
      Compile / libraryDependencies,
      name, version, scalaVersion, sbtVersion
    ),
    buildInfoOptions += BuildInfoOption.BuildTime,
    buildInfoPackage := "etlflow",
    assembly / test := {},
    assembly / assemblyMergeStrategy := {
      case PathList("META-INF", xs @ _*) => MergeStrategy.discard
      case x => MergeStrategy.first
    },
    assembly / assemblyShadeRules := Seq(
      ShadeRule.rename("com.google.common.**" -> "repackaged.com.google.common.@1").inAll
    ),
  )
  .dependsOn(db, utils)

lazy val spark = (project in file("modules/spark"))
  .settings(commonSettings)
  .settings(sparkSettings)
  .dependsOn(core % "compile->compile;test->test",db, utils)

lazy val cloud = (project in file("modules/cloud"))
  .settings(commonSettings)
  .settings(cloudSettings)
  .dependsOn(core % "compile->compile;test->test",db, utils)

lazy val server = (project in file("modules/server"))
  .settings(commonSettings)
  .settings(serverSettings)
  .dependsOn(core % "compile->compile;test->test", cloud, db, utils)

lazy val db = (project in file("modules/db"))
  .settings(commonSettings)
  .settings(dbSettings)
  .dependsOn(utils)

lazy val utils = (project in file("modules/utils"))
  .settings(commonSettings)
  .settings(utilsSettings)

lazy val http = (project in file("modules/http"))
  .settings(commonSettings)
  .settings(httpSettings)
  .dependsOn(core % "compile->compile;test->test", utils)

lazy val redis = (project in file("modules/redis"))
  .settings(commonSettings)
  .settings(redisSettings)
  .dependsOn(core % "compile->compile;test->test", utils)



