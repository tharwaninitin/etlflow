//import NativePackagerHelper._
//import Dependencies._
//
//val EtlFlowVersion = "0.10.0"
//
//lazy val loggerTask = TaskKey[Unit]("loggerTask")
//
////lazy val etlflowCore = ProjectRef(uri("git://github.com/tharwaninitin/etlflow.git#minimal"), "core")
////lazy val etlflowServer = ProjectRef(uri("git://github.com/tharwaninitin/etlflow.git#minimal"), "server")
////lazy val etlflowSpark = ProjectRef(uri("git://github.com/tharwaninitin/etlflow.git#minimal"), "spark")
////lazy val etlflowCloud = ProjectRef(uri("git://github.com/tharwaninitin/etlflow.git#minimal"), "cloud")
//
//lazy val examples = (project in file("examples"))
//  .enablePlugins(JavaAppPackaging)
//  .enablePlugins(DockerPlugin)
//  .settings(
//    name := "examples",
//    organization := "com.github.tharwaninitin",
//    scalaVersion := "2.12.13",
//    libraryDependencies ++= List(
//        "com.github.tharwaninitin" %% "etlflow-core" % EtlFlowVersion,
//        "com.github.tharwaninitin" %% "etlflow-server" % EtlFlowVersion,
//        "com.github.tharwaninitin" %% "etlflow-spark" % EtlFlowVersion,
//        "com.github.tharwaninitin" %% "etlflow-cloud" % EtlFlowVersion,
//        "org.apache.spark" %% "spark-sql" % SparkVersion,
//        "com.google.cloud.spark" %% "spark-bigquery-with-dependencies" % SparkBQVersion,
//        "com.google.cloud.bigdataoss" % "gcs-connector" % HadoopGCSVersion,
//        "org.apache.hadoop" % "hadoop-aws" % HadoopS3Version,
//        "org.apache.hadoop" % "hadoop-common" % HadoopS3Version,
//        "ch.qos.logback" % "logback-classic" % LogbackVersion,
//        "org.postgresql" % "postgresql" % PgVersion,
//    ),
//    loggerTask := {
//        val logger = org.slf4j.LoggerFactory.getLogger("sbt")
//        println(logger.getClass.getName)
//    },
//    dependencyOverrides ++= {
//        Seq(
//            "com.fasterxml.jackson.module" % "jackson-module-scala_2.12" % "2.6.7.1",
//            "com.fasterxml.jackson.core" % "jackson-databind" % "2.6.7.1",
//        )
//    },
//    excludeDependencies ++= Seq(
//        "org.slf4j" % "slf4j-log4j12",
//        //"log4j" % "log4j"
//    ),
//    Docker / packageName  := "etlflow",
//    Compile / mainClass := Some("examples.RunServer"),
//    dockerBaseImage := "openjdk:jre",
//    dockerExposedPorts ++= Seq(8080),
//    maintainer := "tharwaninitin182@gmail.com",
//    // https://stackoverflow.com/questions/40511337/how-copy-resources-files-with-sbt-docker-plugin
//    // mappings.in(Universal) += (sourceDirectory.value / "main" / "conf" / "loaddata.properties", "conf/loaddata.properties"),
//    // mappings.in(Universal) += (sourceDirectory.value / "main" / "conf" / "cred.json", "conf/cred.json"),
//    Universal / mappings ++= directory(sourceDirectory.value / "main" / "data"),
//    Test / parallelExecution := false,
////    assembly / mainClass := Some("examples.LoadData"),
////    assembly / assemblyJarName := "etljobs-examples-assembly_2.12-0.7.19.jar",
////    assembly / assemblyMergeStrategy := {
////       case PathList("META-INF", xs @ _*) => MergeStrategy.discard
////       case x => MergeStrategy.first
////    },
////    assembly / assemblyShadeRules := Seq(
////      ShadeRule.rename("com.google.common.**" -> "repackaged.com.google.common.@1").inAll,
////      ShadeRule.rename("io.grpc.**" -> "repackaged.io.grpc.@1").inAll
////    )
//  )
//  //.dependsOn(etlflowCore, etlflowServer, etlflowSpark, etlflowCloud)
