package etlflow

package object utils {

  sealed trait FSType
  object FSType {
    case object LOCAL extends FSType
    case object GCS extends FSType
  }

  sealed trait LoggingLevel
  object LoggingLevel {
    case object JOB extends LoggingLevel
    case object DEBUG extends LoggingLevel
    case object INFO extends LoggingLevel
  }

  sealed trait Executor
  object Executor {
    case object LOCAL extends Executor
    case class DATAPROC(project: String, region: String, endpoint: String, cluster_name: String) extends Executor
    case class LOCAL_SUBPROCESS(script_path: String,heap_min_memory :String = "-Xms128m", heap_max_memory :String = "-Xmx256m") extends Executor
    case class LIVY(url: String) extends Executor
    case class KUBERNETES(
          imageName: String, nameSpace: String, envVar: Map[String,Option[String]],
          containerName: Option[String] = Some("etljob"),
          entryPoint: Option[String] = Some("/opt/docker/bin/load-data"),
          restartPolicy: Option[String] = Some("Never")
         ) extends Executor
  }

  sealed trait Environment
  object Environment {
    final case class GCP(service_account_key_path: String, project_id: String = "") extends Environment {
      override def toString: String = "****service_account_key_path****"
    }
    final case class AWS(access_key: String, secret_key: String) extends Environment {
      override def toString: String = "****access_key****secret_key****"
    }
    case object LOCAL extends Environment
  }

  final case class SMTP(port: String, host: String, user:String, password:String, transport_protocol:String = "smtp", starttls_enable:String = "true", smtp_auth:String = "true") {
    override def toString: String = s"SMTP with host  => $host and user => $user"
  }

  sealed trait IOType extends Serializable
  final case class CSV(delimiter: String = ",", header_present: Boolean = true, parse_mode: String = "FAILFAST", quotechar: String = "\"") extends IOType {
    override def toString: String = s"CSV with delimiter => $delimiter header_present => $header_present parse_mode => $parse_mode"
  }
  final case class MCSV(delimiter: String, no_of_columns: Int) extends IOType
  final case class JDBC(url: String, user: String, password: String, driver: String) extends IOType {
    override def toString: String = s"JDBC with url => $url"
  }
  final case class REDIS(url: String, user: String, password: String, port: Int) extends IOType {
    override def toString: String = s"REDIS with url $url and port $port"
  }
  final case class JSON(multi_line: Boolean = false) extends IOType {
    override def toString: String = s"Json with multiline  => $multi_line"
  }

  case class Config(dbLog: JDBC, slack: Slack)
  case class Slack(url: String, env: String)

  case object BQ extends IOType
  case object PARQUET extends IOType
  case object ORC extends IOType
  case object TEXT extends IOType
  case object EXCEL extends IOType
}