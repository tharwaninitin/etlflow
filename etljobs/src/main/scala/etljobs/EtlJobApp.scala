package etljobs

import etljobs.utils.{AppLogger, GlobalProperties, UtilityFunctions => UF}
import org.apache.log4j.Logger
import scala.reflect.runtime.universe.TypeTag
import scala.reflect.runtime.{universe => ru}
// Either use =>
// 1) abstract class EtlJobApp[EJN: TypeTag]
// 2) Or below "trait with type" like this => trait EtlJobApp[T] { type EJN = TypeTag[T] }
abstract class EtlJobApp[EJN: TypeTag, EJP: TypeTag] {
  lazy val ea_logger: Logger = Logger.getLogger(getClass.getName)
  val global_properties: Option[GlobalProperties]

  def toEtlJob(job_name: EJN, job_properties: EtlJobProps): EtlJob
  def toEtlJobProps(job_name: EJN, job_properties: Map[String, String]): EtlJobProps
  def toEtlJobPropsAsJson(job_name: EJN): Map[String, String]

  def main(args: Array[String]): Unit = {
    val job_properties = UF.parser(args.drop(1))

    args(0) match {
      case "list_jobs" => UF.printEtlJobs[EJN]
      case "show_job_props" =>
        val job_name  = UF.getEtlJobName[EJN](job_properties("job_name"))
        val json      = UF.convertToJson(toEtlJobPropsAsJson(job_name))
        println(json)
      case "show_job_default_props" =>
        val job_name  = UF.getEtlJobName[EJN](job_properties("job_name"))
        val etl_props = toEtlJobProps(job_name, job_properties)
        val json      = UF.convertToJsonByRemovingKeys(etl_props, List("job_run_id","job_description","job_properties"))
        println(json)
      case "show_step_props" =>
        val job_name  = UF.getEtlJobName[EJN](job_properties("job_name"))
        val etl_props = toEtlJobProps(job_name, job_properties)
        val etl_job   = toEtlJob(job_name, etl_props)
        val json      = UF.convertToJson(etl_job.getJobInfo(etl_job.job_properties.job_notification_level))
        println(json)
      case "run_job" =>
        val job_name  = UF.getEtlJobName[EJN](job_properties("job_name"))
        val etl_props = toEtlJobProps(job_name, job_properties)
        val etl_job   = toEtlJob(job_name, etl_props)
        etl_job.execute
      case _ =>
        ea_logger.error("Unsupported parameter, Supported params are list_jobs, show_job_props, show_step_props, run_job")
        throw EtlJobException("Unsupported parameter, Supported params are list_jobs, show_job_props, show_step_props, run_job")
    }
  }
}

