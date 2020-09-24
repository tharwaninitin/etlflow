lazy val scala212 = "2.12.10"
lazy val supportedScalaVersions = List(scala212)

import Dependencies._

lazy val coreSettings = Seq(
  name := "etlflow-core",
  organization := "com.github.tharwaninitin",
  crossScalaVersions := supportedScalaVersions,
  addCompilerPlugin("org.typelevel" %% "kind-projector" % "0.11.0" cross CrossVersion.full),
  libraryDependencies ++= zioLibs ++ dbLibs ++ catsLibs ++ jsonLibs
    ++ miscLibs ++ redis ++ scalajHttp ++ mail ++ coreTestLibs,
  excludeDependencies ++= Seq(
    "org.slf4j" % "slf4j-log4j12",
  ),
  //https://stackoverflow.com/questions/36501352/how-to-force-a-specific-version-of-dependency
    dependencyOverrides ++= {
    Seq(
      "com.fasterxml.jackson.module" % "jackson-module-scala_2.12" % "2.6.7.1",
      "com.fasterxml.jackson.core" % "jackson-databind" % "2.6.7.1",
    )
  }
)

lazy val cloudSettings = Seq(
  name := "etlflow-cloud",
  organization := "com.github.tharwaninitin",
  crossScalaVersions := supportedScalaVersions,
  addCompilerPlugin("org.typelevel" %% "kind-projector" % "0.11.0" cross CrossVersion.full),
  libraryDependencies ++= streamingLibs ++ googleCloudLibs ++ awsLibs ++ cloudTestLibs,
  excludeDependencies ++= Seq(
    "org.slf4j" % "slf4j-log4j12",
  ),
)

lazy val sparkSettings = Seq(
  name := "etlflow-spark",
  organization := "com.github.tharwaninitin",
  crossScalaVersions := supportedScalaVersions,
  addCompilerPlugin("org.typelevel" %% "kind-projector" % "0.11.0" cross CrossVersion.full),
  libraryDependencies ++= sparkLibs ++ cloudTestLibs,
  excludeDependencies ++= Seq(
    "org.slf4j" % "slf4j-log4j12",
  ),
)

lazy val schedulerSettings = Seq(
  name := "etlflow-scheduler"
  , libraryDependencies ++= caliban ++ jwt ++ kubernetes ++ http4sclient ++ coreTestLibs
)

lazy val root = (project in file("."))
  .settings(
    crossScalaVersions := Nil, // crossScalaVersions must be set to Nil on the aggregating project
    publish / skip := true)
  .aggregate(core,spark,cloud,scheduler)

lazy val core = (project in file("modules/core"))
  .settings(coreSettings)
  .enablePlugins(BuildInfoPlugin)
  .settings(
    initialCommands := "import etlflow._",
    buildInfoKeys := Seq[BuildInfoKey](
      resolvers,
      libraryDependencies in Compile,
      name, version, scalaVersion, sbtVersion
    ),
    buildInfoOptions += BuildInfoOption.BuildTime,
    buildInfoPackage := "etlflow",
    test in assembly := {},
    assemblyMergeStrategy in assembly := {
      case PathList("META-INF", xs @ _*) => MergeStrategy.discard
      case x => MergeStrategy.first
    },
    assemblyShadeRules in assembly := Seq(
      ShadeRule.rename("com.google.common.**" -> "repackaged.com.google.common.@1").inAll
    ),
    Test / parallelExecution := false,
    testFrameworks += (new TestFramework("zio.test.sbt.ZTestFramework"))
  )

lazy val spark = (project in file("modules/spark"))
  .settings(sparkSettings)
  .settings(
    scalacOptions ++= Seq("-Ypartial-unification"),
    Test / parallelExecution := false,
    testFrameworks += (new TestFramework("zio.test.sbt.ZTestFramework"))
  )
  .dependsOn(core % "compile->compile;test->test")

lazy val cloud = (project in file("modules/cloud"))
  .settings(cloudSettings)
  .settings(
    scalacOptions ++= Seq("-Ypartial-unification"),
    Test / parallelExecution := false,
    testFrameworks += (new TestFramework("zio.test.sbt.ZTestFramework"))
  )
  .dependsOn(core % "compile->compile;test->test")

lazy val scheduler = (project in file("modules/scheduler"))
  .settings(schedulerSettings)
  .settings(
    scalacOptions ++= Seq("-Ypartial-unification"),
    Test / parallelExecution := false,
    testFrameworks += (new TestFramework("zio.test.sbt.ZTestFramework"))
  )
  .dependsOn(core % "compile->compile;test->test", cloud)



