package etlflow

import etlflow.db.{DBApi, RunDbMigration, liveDBWithTransactor}
import etlflow.etljobs.EtlJob
import etlflow.executor.LocalExecutor
import etlflow.log.ApplicationLogger
import etlflow.utils.EtlJobArgsParser.{EtlJobConfig, parser}
import etlflow.utils.{Configuration, UtilityFunctions => UF}
import zio.{App, ExitCode, UIO, URIO, ZEnv, ZIO}

import scala.reflect.runtime.universe.TypeTag

abstract class EtlFlowApp[EJN <: EtlJobPropsMapping[EtlJobProps,EtlJob[EtlJobProps]] : TypeTag]
  extends Configuration
    with ApplicationLogger
    with App {

  val etl_job_props_mapping_package: String = UF.getJobNamePackage[EJN] + "$"

  def cliRunner(args: List[String], app: ZIO[ZEnv, Throwable, Unit] = ZIO.fail(new RuntimeException("Extend ServerApp instead of EtlFlowApp")))
  : ZIO[ZEnv,Throwable,Unit] = {
    parser.parse(args, EtlJobConfig()) match {
      case Some(serverConfig) => serverConfig match {
        case ec if ec.init_db =>
          logger.info("Initializing etlflow database")
          RunDbMigration(config.dbLog).unit
        case ec if ec.reset_db =>
          logger.info("Resetting etlflow database")
          RunDbMigration(config.dbLog, clean = true).unit
        case ec if ec.add_user && ec.user != "" && ec.password != "" =>
          logger.info("Inserting user into database")
          val encryptedPassword = UF.encryptKey(ec.password)
          val query = s"INSERT INTO userinfo(user_name,password,user_active,user_role) values (\'${ec.user}\',\'${encryptedPassword}\',\'true\',\'${"admin"}\');"
          val dbLayer = liveDBWithTransactor(config.dbLog)
          DBApi.executeQuery(query).provideCustomLayer(dbLayer)
        case ec if ec.add_user && ec.user == "" =>
          logger.error(s"Need to provide args --user")
          ZIO.fail(new RuntimeException("Need to provide args --user"))
        case ec if ec.add_user && ec.password == "" =>
          logger.error(s"Need to provide args --password")
          ZIO.fail(new RuntimeException("Need to provide args --password"))
        case ec if ec.list_jobs =>
          UIO(UF.printEtlJobs[EJN]())
        case ec if ec.show_job_props && ec.job_name != "" =>
          logger.info(s"""Executing show_job_props with params: job_name => ${ec.job_name}""".stripMargin)
          LocalExecutor(etl_job_props_mapping_package).showJobProps(ec.job_name, ec.job_properties ,etl_job_props_mapping_package)
        case ec if ec.show_step_props && ec.job_name != "" =>
          logger.info(s"""Executing show_step_props with params: job_name => ${ec.job_name} job_properties => ${ec.job_properties}""")
          logger.warn(s"""This command will actually instantiate EtlJob for ${ec.job_name}""")
          LocalExecutor(etl_job_props_mapping_package).showJobStepProps(ec.job_name, ec.job_properties ,etl_job_props_mapping_package)
        case ec if (ec.show_job_props || ec.show_step_props) && ec.job_name == "" =>
          logger.error(s"Need to provide args --job_name")
          ZIO.fail(new RuntimeException("Need to provide args --job_name"))
        case ec if ec.run_job && ec.job_name != "" =>
          logger.info(s"""Running job with params: job_name => ${ec.job_name} job_properties => ${ec.job_properties}""".stripMargin)
          val jri = if(ec.job_properties.keySet.contains("job_run_id")) Some(ec.job_properties("job_run_id")) else None
          val is_master = if(ec.job_properties.keySet.contains("is_master")) Some(ec.job_properties("is_master")) else None
          val dbLayer = liveDBWithTransactor(config.dbLog)
          LocalExecutor(etl_job_props_mapping_package, jri, is_master)
            .executeJob(ec.job_name, ec.job_properties)
            .provideCustomLayer(dbLayer)
        case ec if ec.run_server =>
            logger.info("Starting server")
            app
        case _ =>
          logger.error(s"Incorrect input args or no args provided, Try --help for more information.")
          ZIO.fail(new RuntimeException("Incorrect input args or no args provided, Try --help for more information."))
      }
      case None => ZIO.fail(new RuntimeException("Incorrect input args or no args provided, Try --help for more information."))
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

