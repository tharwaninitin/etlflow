package etlflow.model

sealed trait Executor
object Executor {
  case class DATAPROC(cluster: String, project: String, region: String, endpoint: String, conf: Map[String, String])
      extends Executor
  case class LOCAL_SUBPROCESS(script_path: String, heap_min_memory: String = "-Xms128m", heap_max_memory: String = "-Xmx256m")
      extends Executor
  case class LIVY(url: String) extends Executor
  case class KUBERNETES(
      imageName: String,
      nameSpace: String,
      envVar: Map[String, Option[String]],
      containerName: String = "etljob",
      entryPoint: Option[String] = Some("/opt/docker/bin/load-data"),
      restartPolicy: Option[String] = Some("Never")
  ) extends Executor
  case object LOCAL extends Executor
}
