//package etlflow.coretests.log
//
//import etlflow.coretests.TestSuiteHelper
//import etlflow.coretests.steps.db.DBStepTestSuite.fullLayer
//import etlflow.db.liveDBWithTransactor
//import etlflow.etlsteps.GenericETLStep
//import etlflow.log.SlackLogger
//import etlflow.schema.LoggingLevel
//import org.slf4j.{Logger, LoggerFactory}
//import zio.Runtime.default.unsafeRun
//import zio.{Task, UIO, ZIO}
//import zio.test.Assertion.equalTo
//import zio.test._
//
//import java.util.TimeZone
//
//object SlackLoggingTestSuite extends DefaultRunnableSpec with TestSuiteHelper{
//
//  private val tz = TimeZone.getDefault.getDisplayName(false, TimeZone.SHORT)
//  val slack_url = ""
//  val slack_env = "dev-testing"
//  val host_url = "localhost:8080"
//  val job_name = "EtlSlackJob"
//
//  def processData(ip: Unit): Unit = {
//    logger.info("Hello World")
//  }
//  def processDataFail(ip: Unit): Unit = {
//    logger.info("Hello World")
//    throw new RuntimeException("Failed in processing data")
//  }
//  def cleanSlackMessage(msg: String): String = {
//    msg.replaceAll("[0-9]", "x").replaceAll("\\s+","").replaceAll("x","")
//  }
//
//  val step2 = GenericETLStep(
//    name               = "ProcessData",
//    transform_function = processData,
//  )
//
//  val slack_logger = SlackLogger(job_name, slack_env, slack_url, LoggingLevel.INFO, true, host_url)
//
//  val env = liveDBWithTransactor(config.db) ++ jsonLayer ++ cryptoLayer  ++ etlflow.log.Implementation.live(slack_logger)
//
//  def spec: ZSpec[environment.TestEnvironment, Any] =
//    (suite("EtlFlow Slack Log Suite") (
//      testM("Execute job with log level INFO - Success Case") {
//
//        val step1 = GenericETLStep(
//          name               = "ProcessData",
//          transform_function = processData,
//        )
//
//        val job = step1.execute()
//
//        val message = cleanSlackMessage(f"""
//              :large_blue_circle: dev-testing - EtlSlackJob Process *Success!*
//                *Time of Execution*: xxxx-xx-xx xx:xx:xx$tz
//                *Details Available at*: $host_url
//                *Steps (Task - Duration)*:
//                  :small_blue_diamond:*ProcessData* - (x.xx secs)
//              """.stripMargin)
//
//        val slackInfoLevelExecutor =
//          for {
//            slack  <- JobExecutor.apply(job_name,slack_env,slack_url,job,LoggingLevel.INFO,true,host_url)
//            _      = logger.info("slack :" + slack.final_message)
//          } yield cleanSlackMessage(slack.final_message)
//
//        assertM(slackInfoLevelExecutor)(equalTo(message))
//      }
//    ) @@ TestAspect.sequential).provideCustomLayerShared((env).orDie)
//}
