package etljobs

import etljobs.utils.{AppLogger, GlobalProperties, UtilityFunctions}
import org.apache.log4j.Logger
import scala.reflect.runtime.universe.TypeTag

// Either use => abstract class MyEtlJobApp[EJN: TypeTag] or below trait method with type
trait EtlJobApp[T] extends UtilityFunctions {
  type EJN = TypeTag[T]
  AppLogger.initialize()
  lazy val ea_logger: Logger = Logger.getLogger(getClass.getName)
  val global_properties: Option[GlobalProperties]
  val send_notification: Boolean
  val notification_level: String

  def toEtlJob(job_name: String, job_properties: Map[String, String]): EtlJob

  def main(args: Array[String]): Unit = {
    args(0) match {
      case "list_jobs" => printEtlJobs[EJN]
      case "show_job" =>
        val job_properties = parser(args.drop(1))
        val etl_job = toEtlJob(job_properties("job_name"), job_properties)
        etl_job.etl_step_list.foreach { s =>
          ea_logger.info("=" * 10 + s.name + "=" * 10)
          s.getStepProperties(notification_level).foreach(prop => ea_logger.info(s"==> $prop"))
        }
      case "run_job" =>
        val job_properties = parser(args.drop(1))
        val etl_job = toEtlJob(job_properties("job_name"), job_properties)
        etl_job.execute(send_notification, notification_level)
      case _ =>
        ea_logger.error("Unsupported parameter, Supported params are list_jobs, show_job, run_job")
        throw EtlJobException("Unsupported parameter, Supported params are list_jobs, show_job, run_job")
    }
  }
}

