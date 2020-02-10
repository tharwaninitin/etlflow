import etljobs.utils.GlobalProperties
import org.apache.log4j.Logger
import scala.util.{Failure, Success, Try}

package object etljobs {
  trait EtlJobName
  trait EtlProps {
    val job_run_id: String
    val job_name: EtlJobName
  }
  trait EtlJobManager {
    val etl_job_logger: Logger = Logger.getLogger(getClass.getName)
    def executeEtlJob(etl_job: EtlJob, send_notification: Boolean, notification_level: String): Unit = {
      val output: Try[Unit] = Try {
        etl_job.execute(send_notification, notification_level)
      }
      output match {
        case Success(_) => etl_job_logger.info("Success!!\n")
        case Failure(e) => etl_job_logger.error(e.getMessage + ". Stopping execution!\n")
          throw e
      }
    }
    def toEtlJob(job_name: String, job_properties: Map[String, String], gp: Option[GlobalProperties]): EtlJob
  }
  case class EtlJobException(msg : String) extends Exception
}
