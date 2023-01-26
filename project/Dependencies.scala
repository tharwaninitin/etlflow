import Versions._
import sbt._

object Dependencies {

  lazy val coreLibs = List(
    "dev.zio" %% "zio"               % ZioVersion,
    "dev.zio" %% "zio-logging-slf4j" % ZioLogVersion,
    "dev.zio" %% "zio-json"          % ZioJsonVersion,
    "dev.zio" %% "zio-config"        % zioConfigVersion
  )

  lazy val awsLibs = List(
    "dev.zio"                %% "zio"                         % ZioVersion,
    "dev.zio"                %% "zio-streams"                 % ZioVersion,
    "dev.zio"                %% "zio-interop-reactivestreams" % ZioReactiveStreamsVersion,
    "org.scala-lang.modules" %% "scala-collection-compat"     % ScalaCollectionCompatVersion,
    "software.amazon.awssdk"  % "s3"                          % AwsS3Version
  )

  lazy val gcpLibs = List(
    "dev.zio"                  %% "zio"                % ZioVersion,
    "com.github.tharwaninitin" %% "gcp4zio-gcs"        % Gcp4ZioVersion,
    "com.github.tharwaninitin" %% "gcp4zio-dp"         % Gcp4ZioVersion,
    "com.github.tharwaninitin" %% "gcp4zio-bq"         % Gcp4ZioVersion,
    "com.github.tharwaninitin" %% "gcp4zio-pubsub"     % Gcp4ZioVersion,
    "com.github.tharwaninitin" %% "gcp4zio-monitoring" % Gcp4ZioVersion
  )

  lazy val jdbcLibs = List(
    "dev.zio"         %% "zio"         % ZioVersion,
    "org.scalikejdbc" %% "scalikejdbc" % ScalaLikeJdbcVersion
  )

  lazy val httpLibs = List(
    "dev.zio"                       %% "zio"           % ZioVersion,
    "com.softwaremill.sttp.client3" %% "zio"           % SttpVersion,
    "com.softwaremill.sttp.client3" %% "slf4j-backend" % SttpVersion
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
    "dev.zio"                %% "zio"                     % ZioVersion,
    "org.scala-lang.modules" %% "scala-collection-compat" % ScalaCollectionCompatVersion,
    "io.kubernetes"           % "client-java"             % K8SVersion
  )

  lazy val ftpLibs = List(
    "dev.zio" %% "zio"     % ZioVersion,
    "dev.zio" %% "zio-ftp" % ZioFtpVersion
  )

  lazy val coreTestLibs = List(
    "ch.qos.logback" % "logback-classic"     % LogbackVersion,
    "dev.zio"       %% "zio-test"            % ZioVersion,
    "dev.zio"       %% "zio-test-sbt"        % ZioVersion,
    "dev.zio"       %% "zio-config-typesafe" % zioConfigVersion
  ).map(_ % Test)

  lazy val jdbcTestLibs = List(
    "org.postgresql" % "postgresql"           % PgVersion,
    "mysql"          % "mysql-connector-java" % MySqlVersion,
    "com.h2database" % "h2"                   % "2.1.+"
  ).map(_ % Test)

  lazy val sparkTestLibs = List(
    "org.apache.spark" %% "spark-sql" % SparkVersion
  ).map(_ % Test)
}
