package etlflow

import etlflow.crypto.CryptoApi
import etlflow.db.{DBApi, RunDbMigration}
import etlflow.schema.Config
import etlflow.utils.CliArgsParserAPI
import etlflow.utils.CliArgsParserAPI.EtlJobConfig
import etlflow.utils.CliArgsDBParserAPI
import etlflow.utils.CliArgsDBParserAPI.EtlJobDBConfig
import etlflow.utils.{ApplicationLogger, EtlFlowJobExecutor, Configuration}
import zio._

abstract class EtlFlowApp[T <: EJPMType : Tag]
  extends ApplicationLogger
    with App {

  def cliRunner(args: List[String], config: Config, app: ZIO[ZEnv, Throwable, Unit] = ZIO.fail(new RuntimeException("Extend ServerApp instead of EtlFlowApp")))
  : ZIO[ZEnv,Throwable,Unit] = {
    
    val executor = new EtlFlowJobExecutor[T]
    
    if(config.db.isEmpty) {
      CliArgsParserAPI.parser.parse(args, EtlJobConfig()) match {
        case Some(serverConfig) => serverConfig match {
          case ec if ec.list_jobs =>
            executor.list_jobs
          case ec if ec.show_job_props && ec.job_name != "" =>
            logger.info(s"Executing show_job_props with params: job_name => ${ec.job_name}".stripMargin)
            executor.show_job_props(ec.job_name)
          case ec if ec.show_step_props && ec.job_name != "" =>
            logger.info(s"Executing show_step_props with params: job_name => ${ec.job_name} job_properties => ${ec.job_properties}")
            logger.warn(s"This command will actually instantiate EtlJob for ${ec.job_name}")
            executor.show_step_props(ec.job_name, ec.job_properties)
          case ec if (ec.show_job_props || ec.show_step_props) && ec.job_name == "" =>
            logger.error(s"Need to provide args --job_name")
            ZIO.fail(new RuntimeException("Need to provide args --job_name"))
          case ec if ec.run_job && ec.job_name != "" =>
            logger.info(s"Running job with params: job_name => ${ec.job_name} job_properties => ${ec.job_properties}".stripMargin)
            executor.run_job(ec.job_name, ec.job_properties, config)
          case _ =>
            logger.error(s"Incorrect input args or no args provided, Try --help for more information.")
            ZIO.fail(new RuntimeException("Incorrect input args or no args provided, Try --help for more information."))
        }
        case None => ZIO.fail(new RuntimeException("Incorrect input args or no args provided, Try --help for more information."))
      }
    }
    else {
      CliArgsDBParserAPI.parser.parse(args, EtlJobDBConfig()) match {
        case Some(serverConfig) => serverConfig match {
          case ec if ec.init_db =>
            logger.info("Initializing etlflow database")
            RunDbMigration(config.db.get).unit
          case ec if ec.reset_db =>
            logger.info("Resetting etlflow database")
            RunDbMigration(config.db.get, clean = true).unit
          case ec if ec.add_user && ec.user != "" && ec.password != "" =>
            logger.info("Inserting user into database")
            val key = config.secretkey
            val encryptedPassword = CryptoApi.oneWayEncrypt(ec.password).provideCustomLayer(crypto.Implementation.live(key))
            val query = s"INSERT INTO userinfo(user_name,password,user_active,user_role) values (\'${ec.user}\',\'${unsafeRun(encryptedPassword)}\',\'true\',\'${"admin"}\');"
            val dbLayer = db.liveDB(config.db.get)
            DBApi.executeQuery(query).provideCustomLayer(dbLayer)
          case ec if ec.add_user && ec.user == "" =>
            logger.error(s"Need to provide args --user")
            ZIO.fail(new RuntimeException("Need to provide args --user"))
          case ec if ec.add_user && ec.password == "" =>
            logger.error(s"Need to provide args --password")
            ZIO.fail(new RuntimeException("Need to provide args --password"))
          case ec if ec.list_jobs =>
            executor.list_jobs
          case ec if ec.show_job_props && ec.job_name != "" =>
            logger.info(s"Executing show_job_props with params: job_name => ${ec.job_name}".stripMargin)
            executor.show_job_props(ec.job_name)
          case ec if ec.show_step_props && ec.job_name != "" =>
            logger.info(s"Executing show_step_props with params: job_name => ${ec.job_name} job_properties => ${ec.job_properties}")
            logger.warn(s"This command will actually instantiate EtlJob for ${ec.job_name}")
            executor.show_step_props(ec.job_name, ec.job_properties)
          case ec if (ec.show_job_props || ec.show_step_props) && ec.job_name == "" =>
            logger.error(s"Need to provide args --job_name")
            ZIO.fail(new RuntimeException("Need to provide args --job_name"))
          case ec if ec.run_job && ec.job_name != "" =>
            logger.info(s"Running job with params: job_name => ${ec.job_name} job_properties => ${ec.job_properties}".stripMargin)
            executor.run_job(ec.job_name, ec.job_properties, config)
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
  }

  override def run(args: List[String]): URIO[ZEnv, ExitCode] = {
    Configuration.config.flatMap(cfg =>
      cliRunner(args, cfg, ZIO.unit).catchAll{err =>
        UIO {
          logger.error(err.getMessage)
          err.getStackTrace.foreach(x => logger.error(x.toString))
        }
      }
    )
  }.exitCode
}

