package etljobs

import etljobs.etljob.{EtlJob, GenericEtlJob}
import etljobs.utils.EtlJobArgsParser.{EtlJobConfig, parser}
import etljobs.utils.{GlobalProperties, UtilityFunctions => UF}
import org.apache.log4j.Logger

import scala.reflect.runtime.universe.TypeTag

// Either use =>
// 1) abstract class EtlJobApp[EJN: TypeTag]
// 2) Or below "trait with type" like this => trait EtlJobApp[T] { type EJN = TypeTag[T] }
abstract class EtlJobApp[EJN <: EtlJobName[EJP] : TypeTag, EJP <: EtlJobProps : TypeTag, EJGP <: GlobalProperties : TypeTag] {
  lazy val ea_logger: Logger = Logger.getLogger(getClass.getName)
  val global_properties: Option[EJGP]
  val etl_job_name_package: String

  def toEtlJob(job_name: EJN): (EJP,Option[EJGP]) => EtlJob

  def main(args: Array[String]): Unit = {
    parser.parse(args, EtlJobConfig()) match {
      case Some(serverConfig) => serverConfig match {
        case EtlJobConfig(true,false,false,false,false,false,"",_) => UF.printEtlJobs[EJN]
        case EtlJobConfig(false,default,actual,true,false,false,jobName,jobProps) if jobName != "" =>
          ea_logger.info(s"""Executing show_job_props with params: job_name => $jobName""".stripMargin)
          val job_name = UF.getEtlJobName[EJN](jobName,etl_job_name_package)
          println(UF.convertToJson(job_name.default_properties_map))
          val exclude_keys = List("job_run_id","job_description","job_properties")
          if (default && !actual) {
            println(UF.convertToJsonByRemovingKeys(job_name.default_properties,exclude_keys))
          }
          else if (actual && !default) {
            val props = job_name.getActualProperties(jobProps)
            println(UF.convertToJsonByRemovingKeys(props,exclude_keys))
          }
        case EtlJobConfig(false,false,false,false,true,false,jobName,jobProps) if jobName != "" =>
          ea_logger.info(s"""Executing show_step_props with params: job_name => $jobName job_properties => $jobProps""")
          val job_name = UF.getEtlJobName[EJN](jobName,etl_job_name_package)
          val etl_job = toEtlJob(job_name)(job_name.getActualProperties(jobProps),global_properties)
          if (etl_job.isInstanceOf[GenericEtlJob])
            println("Step Props info not available for generic jobs")
          else
            etl_job.job_name = job_name.toString
            val json = UF.convertToJson(etl_job.getJobInfo(etl_job.job_properties.job_notification_level))
            println(json)
        case EtlJobConfig(false,false,false,false,false,true,jobName,jobProps) if jobName != "" =>
          ea_logger.info(s"""Running job with params: job_name => $jobName job_properties => $jobProps""".stripMargin)
          val job_name = UF.getEtlJobName[EJN](jobName,etl_job_name_package)
          val etl_job = toEtlJob(job_name)(job_name.getActualProperties(jobProps),global_properties)
          etl_job.job_name = job_name.toString
          etl_job.execute()
        case etlJobConfig if (etlJobConfig.show_job_props || etlJobConfig.show_step_props) && etlJobConfig.job_name == "" =>
          ea_logger.error(s"Need to provide args --job_name")
          System.exit(1)
        case _ =>
          ea_logger.error(s"Incorrect input args or no args provided, Try --help for more information.")
          System.exit(1)
      }
      case None => System.exit(1)
    }
  }
}

