import etljobs.etlsteps.StateLessEtlStep
import org.apache.log4j.Logger
import etljobs.utils.{UtilityFunctions => UF}
import scala.reflect.runtime.universe.TypeTag

package object etljobs {
  lazy val etl_jobs_logger: Logger = Logger.getLogger(getClass.getName)

  case class EtlJobException(msg : String) extends RuntimeException(msg)

  abstract class EtlJobName[+EJP : TypeTag] {
    val job_props: EtlJobProps
    val job_props_map: Map[String,String] = UF.getEtlJobProps[EJP]()
    def toEtlJobProps(job_properties: Map[String, String]): EJP
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
    def apply(args: StateLessEtlStep*): List[StateLessEtlStep] = {
      val seq = List(args:_*)
      if (seq.map(x => x.name).distinct.length == seq.map(x => x.name).length)
        seq
      else throw EtlJobException(s"Duplicate step name detected from ${seq.map(x => x.name)}")
    }
  }
}
