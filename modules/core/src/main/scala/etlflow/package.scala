import etlflow.etlsteps.EtlStep
import etlflow.log.{DbLogManager, SlackLogManager}
import etlflow.utils.{UtilityFunctions => UF}
import scala.reflect.runtime.universe.TypeTag

package object etlflow {

  case class EtlJobException(msg : String) extends RuntimeException(msg)
  case class EtlJobNotFoundException(msg : String) extends RuntimeException(msg)

  case class LoggerResource(db: Option[DbLogManager], slack: Option[SlackLogManager])

  abstract class EtlJobName[+EJP : TypeTag] {
    final val default_properties_map: Map[String,String] = UF.getEtlJobProps[EJP]()
    final val job_name_package = getClass.getName
    val default_properties: EJP
    def getActualProperties(job_properties: Map[String, String]): EJP
  }

  trait EtlJobSchema extends Product

  trait EtlJobProps {
    val job_properties: Map[String,String]    = Map.empty
    val job_run_id: String                    = java.util.UUID.randomUUID.toString
    val job_description: String               = job_properties.getOrElse("job_description", "")
    val job_aggregate_error: Boolean          = job_properties.getOrElse("job_aggregate_error", "false").toBoolean
    val job_send_slack_notification: Boolean  = job_properties.getOrElse("job_send_slack_notification", "false").toBoolean
    val job_notification_level: String        = job_properties.getOrElse("job_notification_level", "debug") //info or debug
    val job_schedule: String                  = ""
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
