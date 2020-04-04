package etljobs

import etljobs.utils.EtlJobArgsParser.{EtlJobConfig,parser}
import etljobs.utils.{GlobalProperties, UtilityFunctions => UF}
import org.apache.log4j.Logger
import scala.reflect.runtime.universe.TypeTag

// Either use =>
// 1) abstract class EtlJobApp[EJN: TypeTag]
// 2) Or below "trait with type" like this => trait EtlJobApp[T] { type EJN = TypeTag[T] }
abstract class EtlJobApp[EJN <: EtlJobName[EJP] : TypeTag, EJP <: EtlJobProps : TypeTag] {
  lazy val ea_logger: Logger = Logger.getLogger(getClass.getName)
  val global_properties: Option[GlobalProperties]
  val etl_job_list_package: String

  def toEtlJob(job_name: EJN, job_properties: EJP): EtlJob
  def toEtlJobProps(job_name: EJN, job_properties: Map[String, String]): EJP

  def main(args: Array[String]): Unit = {
    parser.parse(args, EtlJobConfig()) match {
      case Some(serverConfig) => serverConfig match {
        case EtlJobConfig(true,_,_,_,_,_,_) => UF.printEtlJobs[EJN]
        case EtlJobConfig(_,true,_,_,_,jobName,_) if jobName != "" =>
          ea_logger.info(s"""Executing show_job_props with params:
                            | job_name => $jobName""".stripMargin)
          val job_name  = UF.getEtlJobName[EJN](jobName,etl_job_list_package)
          val json      = UF.convertToJson(job_name.job_props_map)
          println(json)
        case EtlJobConfig(_,_,true,_,_,jobName,jobProps) if jobName != "" =>
          ea_logger.info(s"""Executing show_job_default_props with params:
                            | job_name => $jobName
                            | job_properties => $jobProps""".stripMargin)
          val job_name  = UF.getEtlJobName[EJN](jobName,etl_job_list_package)
          val json      = UF.convertToJsonByRemovingKeys(job_name.job_props, List("job_run_id","job_description","job_properties"))
          println(json)
        case EtlJobConfig(_,_,_,true,_,jobName,jobProps) if jobName != "" =>
          ea_logger.info(s"""Executing show_step_props with params:
                            | job_name => $jobName
                            | job_properties => $jobProps""".stripMargin)
          val job_name  = UF.getEtlJobName[EJN](jobName,etl_job_list_package)
          val props     = toEtlJobProps(job_name,jobProps)
          val etl_job   = toEtlJob(job_name,props)
          val json      = UF.convertToJson(etl_job.getJobInfo(etl_job.job_properties.job_notification_level))
          println(json)
        case EtlJobConfig(_,_,_,_,true,jobName,jobProps) if jobName != "" =>
          ea_logger.info(s"""Executing job with params:
                            | job_name => $jobName
                            | job_properties => $jobProps""".stripMargin)
          val job_name  = UF.getEtlJobName[EJN](jobName,etl_job_list_package)
          val props     = toEtlJobProps(job_name,jobProps)
          val etl_job   = toEtlJob(job_name,props)
          etl_job.execute
        case etlJobConfig if etlJobConfig.job_name == "" =>
          ea_logger.error(s"Need to provide job_name")
          System.exit(1)
        case _ =>
          ea_logger.error(s"Incorrect input args, contact support :)")
          System.exit(1)
      }
      case None => System.exit(1)
    }
  }
}

