import etlflow.common.EtlflowError.EtlJobException
import etlflow.db.{DBEnv, TransactorEnv}
import etlflow.etljobs.EtlJob
import etlflow.etlsteps.EtlStep
import etlflow.log.StepReq
import etlflow.utils.{Executor, LoggingLevel}
import zio.Has
import zio.blocking.Blocking
import zio.clock.Clock

import scala.reflect.ClassTag

package object etlflow {

  type JobEnv = TransactorEnv with DBEnv with Blocking with Clock
  type StepEnv = Has[StepReq] with JobEnv
  type EJPMType = EtlJobPropsMapping[EtlJobProps,EtlJob[EtlJobProps]]

  trait EtlJobSchema extends Product
  trait EtlJobProps

  abstract class EtlJobPropsMapping[EJP <: EtlJobProps, EJ <: EtlJob[EJP]](implicit tag_EJ: ClassTag[EJ], tag_EJP: ClassTag[EJP]) {
    val job_description: String               = ""
    val job_schedule: String                  = ""
    val job_max_active_runs: Int              = 10
    val job_deploy_mode: Executor             = Executor.LOCAL
    val job_retries: Int                      = 0
    val job_retry_delay_in_minutes: Int       = 0
    val job_enable_db_logging: Boolean        = true
    val job_send_slack_notification: Boolean  = false
    val job_notification_level: LoggingLevel  = LoggingLevel.INFO

    final val job_name: String                = tag_EJ.toString
    final val job_props_name: String          = tag_EJP.toString

    def getActualProperties(job_properties: Map[String, String]): EJP

    // https://stackoverflow.com/questions/46798242/scala-create-instance-by-type-parameter
    final def etlJob(job_properties: Map[String, String]): EJ = {
      val props = getActualProperties(job_properties)
      tag_EJ.runtimeClass.getConstructor(tag_EJP.runtimeClass).newInstance(props).asInstanceOf[EJ]
    }

    final def getProps: Map[String,Any] = Map(
        "job_name" -> job_name,
        "job_props_name" -> job_props_name,
        "job_description" -> job_description,
        "job_schedule" -> job_schedule,
        "job_deploy_mode" -> job_deploy_mode,
        "job_max_active_runs" -> job_max_active_runs,
        "job_retries" -> job_retries,
        "job_retry_delay_in_minutes" -> job_retry_delay_in_minutes,
        "job_enable_db_logging" -> job_enable_db_logging,
        "job_send_slack_notification" -> job_send_slack_notification,
        "job_notification_level" -> job_notification_level
      )
  }

  object EtlStepList {
    def apply(args: EtlStep[Unit,Unit]*): List[EtlStep[Unit,Unit]] = {
      val seq = List(args:_*)
      if (seq.map(x => x.name).distinct.length == seq.map(x => x.name).length)
        seq
      else throw EtlJobException(s"Duplicate step name detected from ${seq.map(x => x.name)}")
    }
  }
}
