import sbt._
import Versions._

object Dependencies {

  lazy val coreLibs = List(
    "dev.zio" %% "zio"               % ZioVersion,
    "dev.zio" %% "zio-logging-slf4j" % ZioLogVersion
  )

  lazy val awsLibs = List(
    "dev.zio"                %% "zio"                         % ZioVersion,
    "dev.zio"                %% "zio-streams"                 % ZioVersion,
    "dev.zio"                %% "zio-interop-reactivestreams" % ZioReactiveStreamsVersion,
    "org.scala-lang.modules" %% "scala-collection-compat"     % ScalaCollectionCompatVersion,
    "software.amazon.awssdk"  % "s3"                          % AwsS3Version
  )

  lazy val gcpLibs = List(
    "dev.zio"                  %% "zio"         % ZioVersion,
    "com.github.tharwaninitin" %% "gcp4zio-gcs" % Gcp4ZioVersion,
    "com.github.tharwaninitin" %% "gcp4zio-dp"  % Gcp4ZioVersion,
    "com.github.tharwaninitin" %% "gcp4zio-bq"  % Gcp4ZioVersion
  )

  lazy val dbLibs = List(
    "dev.zio"         %% "zio"         % ZioVersion,
    "org.scalikejdbc" %% "scalikejdbc" % ScalaLikeJdbcVersion
  )

  lazy val httpLibs = List(
    "dev.zio"                       %% "zio"                           % ZioVersion,
    "com.softwaremill.sttp.client3" %% "async-http-client-backend-zio" % SttpVersion,
    "com.softwaremill.sttp.client3" %% "slf4j-backend"                 % SttpVersion
  )

  lazy val redisLibs = List(
    "dev.zio"       %% "zio"         % ZioVersion,
    "net.debasishg" %% "redisclient" % RedisVersion
  )

  lazy val sparkLibs = List(
    "dev.zio"          %% "zio"       % ZioVersion,
    "org.apache.spark" %% "spark-sql" % SparkVersion % Provided
  )

  lazy val emailLibs = List(
    "dev.zio"     %% "zio"            % ZioVersion,
    "javax.mail"   % "javax.mail-api" % MailVersion,
    "com.sun.mail" % "javax.mail"     % MailVersion
  )

  lazy val k8sLibs = List(
    "dev.zio"                       %% "zio"                           % ZioVersion,
    "com.coralogix"                 %% "zio-k8s-client"                % K8sVersion,
    "com.softwaremill.sttp.client3" %% "async-http-client-backend-zio" % SttpVersion,
    "com.softwaremill.sttp.client3" %% "slf4j-backend"                 % SttpVersion
  )

  lazy val coreTestLibs = List(
    "ch.qos.logback" % "logback-classic" % LogbackVersion,
    "dev.zio"       %% "zio-test"        % ZioVersion,
    "dev.zio"       %% "zio-test-sbt"    % ZioVersion
  ).map(_ % Test)

  lazy val dbTestLibs = List(
    "org.postgresql" % "postgresql"           % PgVersion,
    "mysql"          % "mysql-connector-java" % MySqlVersion
  ).map(_ % Test)

  lazy val sparkTestLibs = List(
    "org.apache.spark" %% "spark-sql" % SparkVersion
//    "com.google.cloud.bigdataoss" % "gcs-connector" % HadoopGCSVersion,
//    "org.apache.hadoop"           % "hadoop-aws"    % HadoopS3Version,
//    "org.apache.hadoop"           % "hadoop-common" % HadoopS3Version
//    "com.google.cloud.spark" %% "spark-bigquery-with-dependencies" % SparkBQVersion
  ).map(_ % Test)
}
