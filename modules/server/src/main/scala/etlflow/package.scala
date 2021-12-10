
import etlflow.core.CoreLogEnv
import etlflow.etljobs.EtlJob
import etlflow.json.{JsonApi, JsonEnv}
import etlflow.schema.Executor
import io.circe.Encoder
import zio.{Tag, ZIO}

package object etlflow {

  type EJPMType = EtlJobPropsMapping[EtlJobProps,EtlJob[EtlJobProps]]
  type JobEnv = CoreLogEnv with JsonEnv

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
}
