package etlflow.task

import com.google.cloud.dataproc.v1.Job
import gcp4zio.dp._
import zio.RIO
import zio.config._
import ConfigDescriptor._

case class DPSparkJobTask(name: String, args: List[String], mainClass: String, libs: List[String], conf: Map[String, String])
    extends EtlTask[DPJob, Job] {

  override def getMetaData: Map[String, String] = Map(
    "args"      -> args.mkString(" "),
    "mainClass" -> mainClass,
    "libs"      -> libs.mkString(","),
    "conf"      -> conf.mkString(",")
  )

  override protected def process: RIO[DPJob, Job] = {
    logger.info("#" * 100)
    logger.info(s"Starting Dataproc Spark Job")
    DPJob.executeSparkJob(args, mainClass, libs, conf)
  }
}

object DPSparkJobTask {
  val config: ConfigDescriptor[DPSparkJobTask] =
    string("name")
      .zip(list("args")(string))
      .zip(string("mainClass"))
      .zip(list("libs")(string))
      .zip(map("conf")(string))
      .to[DPSparkJobTask]
}
