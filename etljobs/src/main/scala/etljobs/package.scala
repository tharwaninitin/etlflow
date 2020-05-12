import etljobs.etlsteps.EtlStep
import org.apache.log4j.Logger
import etljobs.utils.{UtilityFunctions => UF}
import scala.reflect.runtime.universe.TypeTag

package object etljobs {
  lazy val etl_jobs_logger: Logger = Logger.getLogger(getClass.getName)

  case class EtlJobException(msg : String) extends RuntimeException(msg)

  abstract class EtlJobName[+EJP : TypeTag] {
    final val default_properties_map: Map[String,String] = UF.getEtlJobProps[EJP]()
    final val job_name_package = getClass.getName
    val default_properties: EtlJobProps
    def getActualProperties(job_properties: Map[String, String]): EJP
  }

  trait EtlJobSchema extends Product

  trait EtlJobProps {
    val job_properties: Map[String,String]    = Map.empty
    val job_run_id: String                    = java.util.UUID.randomUUID.toString
    var job_description: String               = job_properties.getOrElse("job_description", "")
    var job_aggregate_error: Boolean          = job_properties.getOrElse("job_aggregate_error", "false").toBoolean
    var job_send_slack_notification: Boolean  = job_properties.getOrElse("job_send_slack_notification", "false").toBoolean
    var job_notification_level: String        = job_properties.getOrElse("job_notification_level", "debug") //info or debug
  }

  object EtlStepList {
    def apply(args: EtlStep[_,_]*): List[EtlStep[_,_]] = {
      val seq = List(args:_*)
      if (seq.map(x => x.name).distinct.length == seq.map(x => x.name).length)
        seq
      else throw EtlJobException(s"Duplicate step name detected from ${seq.map(x => x.name)}")
    }
  }
}
