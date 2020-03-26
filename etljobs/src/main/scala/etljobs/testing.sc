import scala.reflect.runtime.universe._

def classAccessors[T: TypeTag](excludeColumnList: Set[String] = Set.empty):List[(String,String)] =
  typeOf[T].members.collect {
    case m: MethodSymbol if m.isCaseAccessor => (m.name.toString, m.returnType.toString)
    case m: MethodSymbol if (m.isVar || m.isVal) && m.isGetter => (m.name.toString, m.returnType.toString)
  }.toList.filterNot(x => excludeColumnList.contains(x._1))

trait EtlJobName
trait EtlJobProps {
  val job_name: EtlJobName
  val job_properties: Map[String,String]
  val job_run_id: String                    = java.util.UUID.randomUUID.toString
  var job_description: String               = job_properties.getOrElse("job_description", "")
  var job_aggregate_error: Boolean          = job_properties.getOrElse("job_aggregate_error", "false").toBoolean
  var job_send_slack_notification: Boolean  = job_properties.getOrElse("job_send_slack_notification", "false").toBoolean
  var job_notification_level: String        = job_properties.getOrElse("job_notification_level", "debug") //info or debug
}
case class SampleEtlJobProps(
                              job_name: EtlJobName,
                              job_properties: Map[String,String] = Map.empty,
                              job_misc: List[String]
                            ) extends EtlJobProps

classAccessors[SampleEtlJobProps](Set("job_properties","job_name","job_run_id","job_description")).foreach(println(_))