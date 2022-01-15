package etlflow

import crypto4s.Crypto
import etlflow.db.DBApi
import etlflow.db.utils.CreateDB
import etlflow.executor.LocalExecutor
import etlflow.schema.Config
import etlflow.utils.CliArgsParserAPI
import etlflow.utils.CliArgsParserAPI.EtlJobConfig
import etlflow.utils.CliArgsDBParserAPI
import etlflow.utils.CliArgsDBParserAPI.EtlJobDBConfig
import etlflow.utils.{ApplicationLogger, Configuration}
import zio._

abstract class CliApp[T <: EJPMType: Tag] extends ApplicationLogger with App {

  def cliRunner(
      args: List[String],
      config: Config,
      app: ZIO[ZEnv, Throwable, Unit] = ZIO.fail(new RuntimeException("Extend ServerApp instead of EtlFlowApp"))
  ): ZIO[ZEnv, Throwable, Unit] = {

    val executor = LocalExecutor[T]()

    if (config.db.isEmpty) {
      CliArgsParserAPI.parser.parse(args, EtlJobConfig()) match {
        case Some(serverConfig) =>
          serverConfig match {
            case ec if ec.list_jobs =>
              executor.listJobs
            case ec if ec.show_job_props && ec.job_name != "" =>
              logger.info(s"Executing show_job_props with params: job_name => ${ec.job_name}".stripMargin)
              executor.showJobProps(ec.job_name)
            case ec if ec.run_job && ec.job_name != "" =>
              logger.info(
                s"Running job with params: job_name => ${ec.job_name} job_properties => ${ec.job_properties}".stripMargin
              )
              executor.runJob(ec.job_name, ec.job_properties, config)
            case _ =>
              logger.error(s"Incorrect input args or no args provided, Try --help for more information.")
              ZIO.fail(new RuntimeException("Incorrect input args or no args provided, Try --help for more information."))
          }
        case None => ZIO.fail(new RuntimeException("Incorrect input args or no args provided, Try --help for more information."))
      }
    } else {
      CliArgsDBParserAPI.parser.parse(args, EtlJobDBConfig()) match {
        case Some(serverConfig) =>
          serverConfig match {
            case ec if ec.init_db =>
              logger.info("Initializing etlflow database")
              CreateDB().provideLayer(db.liveDB(config.db.get)).unit
            case ec if ec.reset_db =>
              logger.info("Resetting etlflow database")
              CreateDB(true).provideLayer(db.liveDB(config.db.get)).unit
            case ec if ec.add_user && ec.user != "" && ec.password != "" =>
              logger.info("Inserting user into database")
              val key               = config.secretkey
              val encryptedPassword = Crypto(key).oneWayEncrypt(ec.password)
              val query =
                s"INSERT INTO userinfo(user_name,password,user_active,user_role) values (\'${ec.user}\',\'$encryptedPassword\',\'true\',\'${"admin"}\');"
              val dbLayer = db.liveDB(config.db.get)
              DBApi.executeQuery(query).provideCustomLayer(dbLayer)
            case ec if ec.add_user && ec.user == "" =>
              logger.error(s"Need to provide args --user")
              ZIO.fail(new RuntimeException("Need to provide args --user"))
            case ec if ec.add_user && ec.password == "" =>
              logger.error(s"Need to provide args --password")
              ZIO.fail(new RuntimeException("Need to provide args --password"))
            case ec if ec.list_jobs =>
              executor.listJobs
            case ec if ec.show_job_props && ec.job_name != "" =>
              logger.info(s"Executing show_job_props with params: job_name => ${ec.job_name}".stripMargin)
              executor.showJobProps(ec.job_name)
            case ec if ec.run_job && ec.job_name != "" =>
              logger.info(
                s"Running job with params: job_name => ${ec.job_name} job_properties => ${ec.job_properties}".stripMargin
              )
              executor.runJob(ec.job_name, ec.job_properties, config)
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

  override def run(args: List[String]): URIO[ZEnv, ExitCode] =
    Configuration.config.flatMap(cfg =>
      cliRunner(args, cfg, ZIO.unit).catchAll { err =>
        UIO {
          logger.error(err.getMessage)
          err.getStackTrace.foreach(x => logger.error(x.toString))
        }
      }
    ).exitCode
}
