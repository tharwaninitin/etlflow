package etlflow

import doobie.hikari.HikariTransactor
import etlflow.etljobs.{EtlJob, SequentialEtlJob}
import etlflow.jdbc.{DbManager, QueryApi}
import etlflow.utils.EtlJobArgsParser.{EtlJobConfig, parser}
import etlflow.utils.{Config, JsonJackson, UtilityFunctions => UF}
import io.circe.generic.auto._
import org.slf4j.{Logger, LoggerFactory}
import zio.{Managed, Runtime, Task, ZEnv}

import scala.reflect.runtime.universe.TypeTag

// Either use =>
// 1) abstract class EtlJobApp[EJN: TypeTag]
// 2) Or below "trait with type" like this => trait EtlJobApp[T] { type EJN = TypeTag[T] }
abstract class EtlJobApp[EJN <: EtlJobName[EJP] : TypeTag, EJP <: EtlJobProps : TypeTag] extends DbManager{
  lazy val ea_logger: Logger = LoggerFactory.getLogger(getClass.getName)

//  def globalProperties: Option[EJGP]
  val globalProperties:Config = io.circe.config.parser.decode[Config]().toOption.get

  val etl_job_name_package: String = UF.getJobNamePackage[EJN] + "$"

  def toEtlJob(job_name: EJN): (EJP,Config) => EtlJob

  def main(args: Array[String]): Unit = {
    parser.parse(args, EtlJobConfig()) match {
      case Some(serverConfig) => serverConfig match {
        case EtlJobConfig(false,false,false,false,false,false,false,"",_,true,false,"","","") =>
          ea_logger.info("Running flyway migration ")
          Runtime.default.unsafeRun(runDbMigration(globalProperties.dbLog))
        case EtlJobConfig(false,false,false,false,false,false,false,"",_,false,true,user_name,password,user_active) if (user_name != "" && password != "" &&  user_active != "") =>
          ea_logger.info("Inserting user details into userinfo table")
          lazy val db: Managed[Throwable, HikariTransactor[Task]] =
            createDbTransactorManagedJDBC(globalProperties.dbLog, scala.concurrent.ExecutionContext.Implicits.global,  "etljob-userinfo" + "-Pool")
          val query = s"insert into userinfo (user_name,password,user_active) values (\'${user_name}\',\'${password}\',\'${user_active}\');"
          ea_logger.info("query : " + query)
          Runtime.default.unsafeRun(QueryApi.executeQuery(db, query))
        case EtlJobConfig(true,false,false,false,false,false,false,"",_,false,false,"","","") => UF.printEtlJobs[EJN]
        case EtlJobConfig(false,default,actual,true,false,false,false,jobName,jobProps,false,false,"","","") if jobName != "" =>
          ea_logger.info(s"""Executing show_job_props with params: job_name => $jobName""".stripMargin)
          val job_name = UF.getEtlJobName[EJN](jobName,etl_job_name_package)
          println(JsonJackson.convertToJson(job_name.default_properties_map))
          val exclude_keys = List("job_run_id","job_description","job_properties")
          if (default && !actual) {
            println(JsonJackson.convertToJsonByRemovingKeys(job_name.getActualProperties(Map.empty),exclude_keys))
          }
          else if (actual && !default) {
            val props = job_name.getActualProperties(jobProps)
            println(JsonJackson.convertToJsonByRemovingKeys(props,exclude_keys))
          }
        case EtlJobConfig(false,false,false,false,true,false,false,jobName,jobProps,false,false,"","","") if jobName != "" =>
          ea_logger.info(s"""Executing show_step_props with params: job_name => $jobName job_properties => $jobProps""")
          val job_name = UF.getEtlJobName[EJN](jobName,etl_job_name_package)
          val etl_job = toEtlJob(job_name)(job_name.getActualProperties(jobProps),globalProperties)
          if (etl_job.isInstanceOf[SequentialEtlJob]) {
            etl_job.job_name = job_name.toString
            val json = JsonJackson.convertToJson(etl_job.getJobInfo(etl_job.job_properties.job_notification_level))
            println(json)
          }
          else {
            println("Step Props info not available for generic jobs")
          }
        case EtlJobConfig(false,false,false,false,false,true,false,jobName,jobProps,false,false,"","","") if jobName != "" =>
          val runtime: Runtime[ZEnv] = Runtime.default
          ea_logger.info(s"""Running job with params: job_name => $jobName job_properties => $jobProps""".stripMargin)
          val job_name = UF.getEtlJobName[EJN](jobName,etl_job_name_package)
          val etl_job = toEtlJob(job_name)(job_name.getActualProperties(jobProps),globalProperties)
          etl_job.job_name = job_name.toString
          runtime.unsafeRun(etl_job.execute())
        case etlJobConfig if (etlJobConfig.show_job_props || etlJobConfig.show_step_props) && etlJobConfig.job_name == "" =>
          ea_logger.error(s"Need to provide args --job_name")
          System.exit(1)
        case etlJobConfig if ( etlJobConfig.insert_userinfo && etlJobConfig.user_name == "" ) =>
          ea_logger.error(s"Need to provide args --user_name ")
          System.exit(1)
        case etlJobConfig if (etlJobConfig.insert_userinfo && etlJobConfig.password == "" ) =>
          ea_logger.error(s"Need to provide args --password ")
          System.exit(1)
        case etlJobConfig if (etlJobConfig.insert_userinfo  && etlJobConfig.user_active == "" ) =>
          ea_logger.error(s"Need to provide args --user_active ")
          System.exit(1)
        case _ =>
          ea_logger.error(s"Incorrect input args or no args provided, Try --help for more information.")
          System.exit(1)
      }
      case None => System.exit(1)
    }
  }
}

