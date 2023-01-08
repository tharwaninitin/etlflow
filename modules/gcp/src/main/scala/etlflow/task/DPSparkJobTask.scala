package etlflow.task

import com.google.cloud.dataproc.v1.Job
import gcp4zio.dp._
import zio.RIO

case class DPSparkJobTask(name: String, args: List[String], mainClass: String, libs: List[String], conf: Map[String, String])
    extends EtlTask[DPJob, Job] {

  override protected def process: RIO[DPJob, Job] = {
    logger.info("#" * 100)
    logger.info(s"Starting Dataproc Spark Job")
    DPJob.executeSparkJob(args, mainClass, libs, conf)
  }

  override def getTaskProperties: Map[String, String] = Map(
    "args"      -> args.mkString(" "),
    "mainClass" -> mainClass,
    "libs"      -> libs.mkString(","),
    "conf"      -> conf.mkString(",")
  )
}
