package etlflow

package object utils {

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
          containerName: String = "etljob",
          entryPoint: Option[String] = Some("/opt/docker/bin/load-data"),
          restartPolicy: Option[String] = Some("Never")
         ) extends Executor
  }

  case class Config(dbLog: Credential.JDBC, timezone: Option[String], slack: Option[Slack], dataProc: Option[DataprocSpark], token: Option[List[String]],webserver: Option[WebServer], host: Option[String])
  case class DataprocSpark(mainClass: String, depLibs: List[String])
  case class Slack(url: String, env: String)
  case class WebServer(ip_address:Option[String],port:Option[Int],secretKey:Option[String])

}