package etlflow.coretests.log

import java.util.TimeZone
import etlflow.utils.LoggingLevel
import etlflow.etlsteps.GenericETLStep
import etlflow.utils.LoggingLevel
import org.slf4j.{Logger, LoggerFactory}
import zio.test.Assertion.equalTo
import zio.test._

object SlackLoggingTestSuite extends DefaultRunnableSpec {

  lazy val logger: Logger = LoggerFactory.getLogger(getClass.getName)

  private val tz = TimeZone.getDefault.getDisplayName(false, TimeZone.SHORT)

  val slack_url = ""
  val slack_env = "dev-testing"
  val host_url = "localhost:8080"
  val job_name = "EtlSlackJob"

  def processData(ip: Unit): Unit = {
    logger.info("Hello World")
  }
  def processDataFail(ip: Unit): Unit = {
    logger.info("Hello World")
    throw new RuntimeException("Failed in processing data")
  }
  def cleanSlackMessage(msg: String): String = {
    msg.replaceAll("[0-9]", "x").replaceAll("\\s+","").replaceAll("x","")
  }

  val step2 = GenericETLStep(
    name               = "ProcessData",
    transform_function = processData,
  )

  def spec: ZSpec[environment.TestEnvironment, Any] =
  suite("EtlFlow Slack Log Suite") (
    testM("Execute job with log level INFO - Success Case") {

      val step1 = GenericETLStep(
        name               = "ProcessData",
        transform_function = processData,
      )

      val job = step1.execute()

      val message = cleanSlackMessage(f"""
              :large_blue_circle: dev-testing - EtlSlackJob Process *Success!*
                *Time of Execution*: xxxx-xx-xx xx:xx:xx$tz
                *Details Available at*: $host_url
                *Steps (Task - Duration)*:
                  :small_blue_diamond:*ProcessData* - (x.xx secs)
              """.stripMargin)

      val slackInfoLevelExecutor =
        for {
          slack  <- JobExecutor.apply(job_name,slack_env,slack_url,job,LoggingLevel.INFO,true,host_url)
        } yield cleanSlackMessage(slack.final_message)

      assertM(slackInfoLevelExecutor)(equalTo(message))
    },
    testM("Execute job with log level INFO - Failure Case") {

      val step1 = GenericETLStep(
        name               = "ProcessData",
        transform_function = processDataFail,
      )

      val job = step1.execute()

      val message = cleanSlackMessage(f"""
                    :red_circle: dev-testing - EtlSlackJob Process *Failed!*
                    *Time of Execution*: xxxx-xx-xx xx:xx:xx$tz
                    *Details Available at*: $host_url
                    *Steps (Task - Duration)*:
                      :small_orange_diamond:*ProcessData* - (x.xx secs)
			                  error -> Failed in processing data
                    """.stripMargin)

      val slackInfoLevelExecutor =
        for {
          slack  <- JobExecutor.apply(job_name,slack_env,slack_url,job,LoggingLevel.INFO,true,host_url)
        } yield cleanSlackMessage(slack.final_message)

      assertM(slackInfoLevelExecutor)(equalTo(message))
    },
    testM("Execute job with log level DEBUG - Success case") {

      val step1 = GenericETLStep(
        name               = "ProcessData",
        transform_function = processData,
      )

      val job =
        for {
          _   <- step1.execute()
          _   <- step2.execute()
        } yield ()

      val debugMessage = cleanSlackMessage(s""":large_blue_circle: dev-testing - EtlSlackJob Process *Success!*
                           |          *Time of Execution*: xxxx-xx-xx xx:xx:xx$tz
                           |          *Details Available at*: $host_url
                           |          *Steps (Task - Duration)*:
                           | :small_blue_diamond:*ProcessData* - (x.xx secs)
                           |
                           | :small_blue_diamond:*ProcessData* - (x.xx secs)
                           |""".stripMargin)

      val slackMessageResult =
        for {
          slack  <- JobExecutor.apply(job_name,slack_env,slack_url,job,LoggingLevel.DEBUG,true,host_url)
        } yield cleanSlackMessage(slack.final_message)

      assertM(slackMessageResult)(equalTo(debugMessage))
    },
    testM("Execute job with log level DEBUG - Failure case") {

      val step1 = GenericETLStep(
        name               = "ProcessData",
        transform_function = processDataFail,
      )

      val job =
        for {
          _   <- step1.execute()
          _   <- step2.execute()
        } yield ()


      val debugFailureMessage = cleanSlackMessage(s""":red_circle: dev-testing - EtlSlackJob Process *Failed!*
                                  |          *Time of Execution*: xxxx-xx-xx xx:xx:xx$tz
                                  |          *Details Available at*: $host_url
                                  |          *Steps (Task - Duration)*:
                                  | :small_orange_diamond:*ProcessData* - (x.xx secs)
                                  |			 , error -> Failed in processing data""".stripMargin)

      val slackDebugLevelExecutor =
        for {
          slack  <- JobExecutor.apply(job_name,slack_env,slack_url,job,LoggingLevel.DEBUG,true,host_url)
        } yield cleanSlackMessage(slack.final_message)

      assertM(slackDebugLevelExecutor)(equalTo(debugFailureMessage))
    },
    testM("Execute job with log level JOB - Success Case") {

      val step1 = GenericETLStep(
        name               = "ProcessData",
        transform_function = processData,
      )

      val job = step1.execute()

      val message = cleanSlackMessage(s""":large_blue_circle: dev-testing - EtlSlackJob Process *Success!*
                      |          *Time of Execution*: xxxx-xx-xx xx:xx:xx$tz*Details Available at*: $host_url""".stripMargin)

      val slackJobLevelExecutor =
        for {
          slack  <- JobExecutor.apply(job_name,slack_env,slack_url,job,LoggingLevel.JOB,true,host_url)
        } yield cleanSlackMessage(slack.final_message)

      assertM(slackJobLevelExecutor)(equalTo(message))
    },
    testM("Execute job with log level JOB - Failure Case") {


      val step1 = GenericETLStep(
        name               = "ProcessData",
        transform_function = processDataFail,
      )

      val job = step1.execute()

      val message = cleanSlackMessage(s""":red_circle: dev-testing - EtlSlackJob Process *Failed!*
                      |          *Time of Execution*: xxxx-xx-xx xx:xx:xx$tz
                      |          *Details Available at*: $host_url
                      |          *Steps (Task - Duration)*:
                      |                      :small_orange_diamond:*ProcessData* - (x.xx secs)
                      |			                  error -> Failed in processing data""".stripMargin)

      val slackJobLevelExecutor =
        for {
          slack  <- JobExecutor.apply(job_name,slack_env,slack_url,job,LoggingLevel.JOB,true,host_url)
        } yield cleanSlackMessage(slack.final_message)

      assertM(slackJobLevelExecutor)(equalTo(message))
    }
  ) @@ TestAspect.sequential
}
