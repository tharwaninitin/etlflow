//package etlflow.coretests.log
//
//import etlflow._
//import etlflow.log.DbStepLogger.{StepReq, logError, logInit, logSuccess}
//import etlflow.log.SlackLogger
//import etlflow.utils.{Configuration, DbManager, LoggingLevel, UtilityFunctions => UF}
//import zio.{UIO, ZEnv, ZIO, ZLayer}
//
//object JobExecutor extends Configuration with DbManager {
//  def apply(job_name: String, slack_env: String, slack_url: String, job: ZIO[StepEnv, Throwable, Unit], job_notification_level:LoggingLevel, job_send_slack_notification:Boolean, host_url:String)
//  : ZIO[ZEnv , Throwable, SlackLogger] = {
//    val slack = for {
//      job_start_time  <- UIO.succeed(UF.getCurrentTimestamp)
//      jri             = java.util.UUID.randomUUID.toString
//      master_job      = "true"
//      slack           = SlackLogger(job_name, slack_env, slack_url, job_notification_level, job_send_slack_notification,host_url)
//      step_layer      = ZLayer.succeed(StepReq(jri,slack))
//      _               <- logInit(job_start_time)
//      _               <- job.provideSomeLayer[JobEnv](step_layer).foldM(
//                            ex => logError(job_start_time,ex).orElse(ZIO.unit),
//                            _  => logSuccess(job_start_time)
//                         )
//    } yield slack.get
//    slack.provideCustomLayer(liveTransactor(config.dbLog))
//  }
//}
