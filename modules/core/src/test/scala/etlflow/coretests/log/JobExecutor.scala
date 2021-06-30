//package etlflow.coretests.log
//
//import etlflow.common.DateTimeFunctions.getCurrentTimestamp
//import etlflow.log.StepLogger.{LoggingSupport, StepLoggerImpl, StepLoggerResourceEnv, logError, logInit, logSuccess}
//import etlflow.log.{SlackLogger, StepReq}
//import etlflow.utils.{Configuration, LoggingLevel}
//import etlflow.{JobEnv, _}
//import zio.{Has, UIO, ZIO, ZLayer}
//
//object JobExecutor extends Configuration  {
//
//
//  def apply(job_name: String, slack_env: String, slack_url: String, job: ZIO[StepEnv, Throwable, Unit], job_notification_level:LoggingLevel, job_send_slack_notification:Boolean, host_url:String):
//  ZIO[LoggingSupport with JobEnv, Throwable, SlackLogger]
//  = {
//    val env: ZLayer[Has[StepReq], Throwable, LoggingSupport] = StepLoggerResourceEnv.live
//
//    val slack = for {
//      job_start_time  <- UIO.succeed(getCurrentTimestamp)
//      jri             = java.util.UUID.randomUUID.toString
//      slack           = SlackLogger(job_name, slack_env, slack_url, job_notification_level, job_send_slack_notification,host_url)
//      step_layer      = ZLayer.succeed(StepReq(jri,slack))
//      _               <- logInit(job_start_time)
//      _               <- job.provideSomeLayer[JobEnv](step_layer).foldM(
//                            ex => logError(job_start_time,ex).orElse(ZIO.unit),
//                            _  => logSuccess(job_start_time)
//                         )
//    } yield slack.get
//    slack
//  }
//}
