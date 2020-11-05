package etlflow

import etlflow.etljobs.SequentialEtlJob
import etlflow.executor.{LocalExecutor, LocalExecutorService}
import etlflow.jdbc.{DbManager, QueryApi}
import etlflow.log.ApplicationLogger
import etlflow.utils.EtlJobArgsParser.{EtlJobConfig, parser}
import etlflow.utils.{Configuration, JsonJackson, UtilityFunctions => UF}
import zio.{App, ExitCode, UIO, URIO, ZEnv, ZIO}
import scala.reflect.runtime.universe.TypeTag

// Either use =>
// 1) abstract class EtlJobApp[EJN: TypeTag]
// 2) Or below "trait with type" like this => trait EtlJobApp[T] { type EJN = TypeTag[T] }
abstract class EtlFlowApp[EJN <: EtlJobName[EJP] : TypeTag, EJP <: EtlJobProps : TypeTag]
  extends DbManager
    with Configuration
    with ApplicationLogger
    with App {

  val etl_job_name_package: String = UF.getJobNamePackage[EJN] + "$"

  def cliRunner(args: List[String], app: ZIO[ZEnv, Throwable, Unit] = ZIO.unit): ZIO[ZEnv,Throwable,Unit] = {
    parser.parse(args, EtlJobConfig()) match {
      case Some(serverConfig) => serverConfig match {
        case ec if ec.run_db_migration =>
          logger.info("Running database migration")
          runDbMigration(config.dbLog) *> ZIO.unit
        case ec if ec.add_user && ec.user != "" && ec.password != "" =>
          logger.info("Inserting user into database")
          val db = createDbTransactorManaged(config.dbLog, platform.executor.asEC,  "AddUser-Pool")
          val query = s"INSERT INTO userinfo (user_name,password,user_active) values (\'${ec.user}\',\'${ec.password}\',\'true\');"
          logger.info("Query: " + query)
          QueryApi.executeQuery(db, query)
        case ec if ec.add_user && ec.user == "" =>
          logger.error(s"Need to provide args --user")
          ZIO(ExitCode.failure)
        case ec if ec.add_user && ec.password == "" =>
          logger.error(s"Need to provide args --password")
          ZIO(ExitCode.failure)
        case ec if ec.list_jobs =>
          UIO(UF.printEtlJobs[EJN])
        case ec if ec.show_job_props && ec.job_name != "" =>
          logger.info(s"""Executing show_job_props with params: job_name => ${ec.job_name}""".stripMargin)
          val job_name = UF.getEtlJobName[EJN](ec.job_name,etl_job_name_package)
          val exclude_keys = List("job_run_id","job_description","job_properties")
          if (ec.actual_values && !ec.default_values) {
            val props = job_name.getActualProperties(ec.job_properties)
            UIO(println(JsonJackson.convertToJsonByRemovingKeys(props,exclude_keys)))
          }
          else {
            val props = job_name.getActualProperties(Map.empty)
            UIO(println(JsonJackson.convertToJsonByRemovingKeys(props,exclude_keys)))
          }
        case ec if ec.show_step_props && ec.job_name != "" =>
          logger.info(s"""Executing show_step_props with params: job_name => ${ec.job_name} job_properties => ${ec.job_properties}""")
          val job_name = UF.getEtlJobName[EJN](ec.job_name,etl_job_name_package)
          val etl_job = job_name.etlJob(ec.job_properties)
          if (etl_job.isInstanceOf[SequentialEtlJob[_]]) {
            etl_job.job_name = job_name.toString
            val json = JsonJackson.convertToJson(etl_job.getJobInfo(etl_job.job_properties.job_notification_level))
            UIO(println(json))
          }
          else {
            UIO(println("Step Props info not available for generic jobs"))
          }
        case ec if (ec.show_job_props || ec.show_step_props) && ec.job_name == "" =>
          logger.error(s"Need to provide args --job_name")
          ZIO(ExitCode.failure)
        case ec if ec.run_job && ec.job_name != "" =>
          logger.info(s"""Running job with params: job_name => ${ec.job_name} job_properties => ${ec.job_properties}""".stripMargin)
          LocalExecutorService.executeLocalJob(ec.job_name, ec.job_properties ,etl_job_name_package,Some(ec.job_properties("job_run_id"))).provideLayer(LocalExecutor.live)
//        case ec if ec.run_server =>
//          if (ec.migration) {
//            logger.info("Running server with migration")
//            runDbMigration(config.dbLog) *> app
//          } else {
//            logger.info("Starting server")
//            app
//          }
        case _ =>
          logger.error(s"Incorrect input args or no args provided, Try --help for more information.")
          ZIO(ExitCode.failure)
      }
      case None => ZIO(ExitCode.failure)
    }
  }

  override def run(args: List[String]): URIO[ZEnv, ExitCode] = {
    cliRunner(args, ZIO.unit).catchAll{err =>
      UIO {
        logger.error(err.getMessage)
        err.getStackTrace.foreach(x => logger.error(x.toString))
      }.exitCode
    }
  }.exitCode
}

