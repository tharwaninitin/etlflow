import sbt._
import Versions._

object Dependencies {

  lazy val coreLibs = List(
    "dev.zio"  %% "zio"       % ZioVersion,
    "org.slf4j" % "slf4j-api" % Sl4jVersion
  )

  lazy val cloudLibs = List(
    "dev.zio"                  %% "zio"                % ZioVersion,
    "dev.zio"                  %% "zio-interop-cats"   % ZioCatsInteropVersion,
    "co.fs2"                   %% "fs2-core"           % Fs2Version,
    "co.fs2"                   %% "fs2-io"             % Fs2Version,
    "org.typelevel"            %% "cats-core"          % CatsCoreVersion,
    "org.typelevel"            %% "cats-effect"        % CatsEffectVersion,
    "org.typelevel"            %% "cats-effect-kernel" % CatsEffectVersion,
    "org.typelevel"            %% "cats-effect-std"    % CatsEffectVersion,
    "com.github.fs2-blobstore" %% "s3"                 % Fs2BlobStoreVersion,
    "com.github.fs2-blobstore" %% "gcs"                % Fs2BlobStoreVersion
  )

  lazy val awsLibs = List(
    "dev.zio"               %% "zio" % ZioVersion,
    "software.amazon.awssdk" % "s3"  % AwsS3Version
  )

  lazy val gcpLibs = List(
    "dev.zio"                  %% "zio"     % ZioVersion,
    "com.github.tharwaninitin" %% "gcp4zio" % Gcp4ZioVersion
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

  lazy val serverLibs = List(
    "dev.zio"                  %% "zio"                 % ZioVersion,
    "dev.zio"                  %% "zio-config"          % ZioConfig,
    "dev.zio"                  %% "zio-config-typesafe" % ZioConfig,
    "dev.zio"                  %% "zio-json"            % ZioJsonVersion,
    "com.github.ghostdogpr"    %% "caliban"             % CalibanVersion,
    "com.github.ghostdogpr"    %% "caliban-zio-http"    % CalibanVersion,
    "io.d11"                   %% "zhttp"               % ZioHttpVersion,
    "com.github.jwt-scala"     %% "jwt-core"            % JwtCoreVersion,
    "org.ocpsoft.prettytime"    % "prettytime"          % PrettyTimeVersion,
    "com.github.scopt"         %% "scopt"               % ScoptVersion,
    "com.github.tharwaninitin" %% "cron4zio"            % Cron4zioVersion,
    "com.github.tharwaninitin" %% "gcp4zio"             % Gcp4ZioVersion,
    "com.github.tharwaninitin" %% "crypto4s"            % Crypto4sVersion,
    "com.github.tharwaninitin" %% "cache4s"             % Cache4sVersion
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
    "org.apache.spark"           %% "spark-sql"     % SparkVersion,
    "com.google.cloud.bigdataoss" % "gcs-connector" % HadoopGCSVersion,
    "org.apache.hadoop"           % "hadoop-aws"    % HadoopS3Version,
    "org.apache.hadoop"           % "hadoop-common" % HadoopS3Version
    // "com.google.cloud.spark" %% "spark-bigquery-with-dependencies" % SparkBQVersion
  ).map(_ % Test)
}
