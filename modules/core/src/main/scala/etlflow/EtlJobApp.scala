package etlflow

import etlflow.etljobs.SequentialEtlJob
import etlflow.jdbc.{DbManager, QueryApi}
import etlflow.utils.EtlJobArgsParser.{EtlJobConfig, parser}
import etlflow.utils.{Configuration, JsonJackson, UtilityFunctions => UF}
import org.slf4j.{Logger, LoggerFactory}
import zio.{Runtime, ZEnv}
import scala.reflect.runtime.universe.TypeTag

// Either use =>
// 1) abstract class EtlJobApp[EJN: TypeTag]
// 2) Or below "trait with type" like this => trait EtlJobApp[T] { type EJN = TypeTag[T] }
abstract class EtlJobApp[EJN <: EtlJobName[EJP] : TypeTag, EJP <: EtlJobProps : TypeTag] extends DbManager with Configuration {
  final lazy val ea_logger: Logger = LoggerFactory.getLogger(getClass.getName)
  final lazy val runtime: Runtime[ZEnv] = Runtime.default
  val etl_job_name_package: String = UF.getJobNamePackage[EJN] + "$"

  def main(args: Array[String]): Unit = {
    parser.parse(args, EtlJobConfig()) match {
      case Some(serverConfig) => serverConfig match {
        case EtlJobConfig(false,false,false,false,false,false,"",_,true,false,"","") =>
          ea_logger.info("Running database migration")
          runtime.unsafeRun(runDbMigration(config.dbLog))
        case EtlJobConfig(false,false,false,false,false,false,"",_,false,true,user_name,password) if user_name != "" && password != "" =>
          ea_logger.info("Inserting user into database")
          val db = createDbTransactorManaged(config.dbLog, runtime.platform.executor.asEC,  "AddUser-Pool")
          val query = s"INSERT INTO userinfo (user_name,password,user_active) values (\'$user_name\',\'$password\',\'true\');"
          ea_logger.info("Query: " + query)
          runtime.unsafeRun(QueryApi.executeQuery(db, query))
        case EtlJobConfig(true,false,false,false,false,false,"",_,false,false,"","") =>
          UF.printEtlJobs[EJN]
        case EtlJobConfig(false,default,actual,true,false,false,jobName,jobProps,false,false,"","") if jobName != "" =>
          ea_logger.info(s"""Executing show_job_props with params: job_name => $jobName""".stripMargin)
          val job_name = UF.getEtlJobName[EJN](jobName,etl_job_name_package)
          val exclude_keys = List("job_run_id","job_description","job_properties")
          if (actual && !default) {
            val props = job_name.getActualProperties(jobProps)
            println(JsonJackson.convertToJsonByRemovingKeys(props,exclude_keys))
          }
          else {
            val props = job_name.getActualProperties(Map.empty)
            println(JsonJackson.convertToJsonByRemovingKeys(props,exclude_keys))
          }
        case EtlJobConfig(false,false,false,false,true,false,jobName,jobProps,false,false,"","") if jobName != "" =>
          ea_logger.info(s"""Executing show_step_props with params: job_name => $jobName job_properties => $jobProps""")
          val job_name = UF.getEtlJobName[EJN](jobName,etl_job_name_package)
          val etl_job = job_name.etlJob(jobProps)
          if (etl_job.isInstanceOf[SequentialEtlJob[_]]) {
            etl_job.job_name = job_name.toString
            val json = JsonJackson.convertToJson(etl_job.getJobInfo(etl_job.job_properties.job_notification_level))
            println(json)
          }
          else {
            println("Step Props info not available for generic jobs")
          }
        case EtlJobConfig(false,false,false,false,false,true,jobName,jobProps,false,false,"","") if jobName != "" =>
          ea_logger.info(s"""Running job with params: job_name => $jobName job_properties => $jobProps""".stripMargin)
          val job_name = UF.getEtlJobName[EJN](jobName,etl_job_name_package)
          val etl_job = job_name.etlJob(jobProps)
          etl_job.job_name = job_name.toString
          runtime.unsafeRun(etl_job.execute())
        case etlJobConfig if (etlJobConfig.show_job_props || etlJobConfig.show_step_props) && etlJobConfig.job_name == "" =>
          ea_logger.error(s"Need to provide args --job_name")
          System.exit(1)
        case etlJobConfig if etlJobConfig.add_user && etlJobConfig.user == "" =>
          ea_logger.error(s"Need to provide args --user")
          System.exit(1)
        case etlJobConfig if etlJobConfig.add_user && etlJobConfig.password == "" =>
          ea_logger.error(s"Need to provide args --password")
          System.exit(1)
        case _ =>
          ea_logger.error(s"Incorrect input args or no args provided, Try --help for more information.")
          System.exit(1)
      }
      case None => System.exit(1)
    }
  }
}

