
import etlflow.core.CoreEnv
import etlflow.etljobs.EtlJob
import etlflow.etlsteps.EtlStep
import etlflow.json.{JsonApi, JsonEnv}
import etlflow.log.{DBLogEnv, LogWrapperEnv}
import etlflow.schema.Executor
import etlflow.utils.EtlflowError.EtlJobException
import io.circe.Encoder
import zio.{Tag, ZIO}

package object etlflow {

  type EJPMType = EtlJobPropsMapping[EtlJobProps,EtlJob[EtlJobProps]]
  type JobEnv = CoreEnv with LogWrapperEnv with DBLogEnv with JsonEnv

  trait EtlJobProps extends Product

  abstract class EtlJobPropsMapping[EJP <: EtlJobProps, EJ <: EtlJob[EJP]](implicit tag_EJ: Tag[EJ], tag_EJP: Tag[EJP], encoder: Encoder[EJP]) {
    val job_description: String               = ""
    val job_schedule: String                  = ""
    val job_max_active_runs: Int              = 10
    val job_deploy_mode: Executor             = Executor.LOCAL
    val job_retries: Int                      = 0
    val job_retry_delay_in_minutes: Int       = 0

    final val job_name: String                = tag_EJ.tag.longName
    final val job_props_name: String          = tag_EJP.tag.longName

    def getActualProperties(job_properties: Map[String, String]): EJP

    final def etlJob(job_properties: Map[String, String]): EJ = {
      val props = getActualProperties(job_properties)
      tag_EJ.closestClass.getConstructor(tag_EJP.closestClass).newInstance(props).asInstanceOf[EJ]
    }

    final def getActualPropertiesAsJson(job_properties: Map[String, String]): ZIO[JsonEnv, Throwable, String] = {
      JsonApi.convertToString(getActualProperties(job_properties), List.empty)
    }

    final def getProps: Map[String,String] = Map(
      "job_name" -> job_name,
      "job_props_name" -> job_props_name,
      "job_description" -> job_description,
      "job_schedule" -> job_schedule,
      "job_deploy_mode" -> job_deploy_mode.toString,
      "job_max_active_runs" -> job_max_active_runs.toString,
      "job_retries" -> job_retries.toString,
      "job_retry_delay_in_minutes" -> job_retry_delay_in_minutes.toString,
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