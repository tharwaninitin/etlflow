package etlflow.log

import etlflow.coretests.TestSuiteHelper
import etlflow.db.liveDB
import etlflow.etlsteps.GenericETLStep
import etlflow.schema.{LoggingLevel, Slack}
import zio.test.Assertion.equalTo
import zio.test._
import zio.{UIO, ZIO}

import java.util.TimeZone

object SlackLoggingTestSuite extends DefaultRunnableSpec with TestSuiteHelper {

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
    msg.replaceAll("[0-9]", "x").replaceAll("\\s+", "").replaceAll("x", "")
  }

  val step2 = GenericETLStep(
    name = "ProcessData",
    transform_function = processData,
  )

  val env = liveDB(config.db) ++ jsonLayer ++ cryptoLayer

  def spec: ZSpec[environment.TestEnvironment, Any] =
    (suite("EtlFlow Slack Log Suite")(
      testM("Execute job with log level INFO - Success Case") {

        val step1 = GenericETLStep(
          name = "ProcessData",
          transform_function = processData,
        )

        val jri = java.util.UUID.randomUUID.toString
        val output_host_url = s"localhost:8080/JobRunDetails/$jri"

        val job = step1.execute()

        val message = cleanSlackMessage(
          f"""
              :large_blue_circle: dev-testing - EtlSlackJob Process *Success!*
                *Time of Execution*: xxxx-xx-xx xx:xx:xx$tz
                *Details Available at*: $output_host_url
                *Steps (Task - Duration)*:
                  :small_blue_diamond:*ProcessData* - (x.xx secs)
              """.stripMargin)

        val slackInfoLevelExecutor =
          for {
            slack <- JobExecutor(job_name, jri, Some(Slack(slack_url,slack_env,host_url)), job, LoggingLevel.INFO, true).tapError(ex => UIO(logger.error(ex.getMessage)))
            _     = logger.info("slack :" + slack.final_message)
          } yield cleanSlackMessage(slack.final_message)

        assertM(slackInfoLevelExecutor)(equalTo(message))
      },
      testM("Execute job with log level INFO - Failure Case") {

        val jri = java.util.UUID.randomUUID.toString
        val output_host_url = s"localhost:8080/JobRunDetails/$jri"

        val step1 = GenericETLStep(
          name               = "ProcessData",
          transform_function = processDataFail,
        )

        val job = step1.execute()

        val message = cleanSlackMessage(f"""
                    :red_circle: dev-testing - EtlSlackJob Process *Failed!*
                    *Time of Execution*: xxxx-xx-xx xx:xx:xx$tz
                    *Details Available at*: $output_host_url
                    *Steps (Task - Duration)*:
                      :small_orange_diamond:*ProcessData* - (x.xx secs)
			                  error -> Failed in processing data
                    """.stripMargin)

        val slackInfoLevelExecutor =
          for {
            slack <- JobExecutor(job_name, jri, Some(Slack(slack_url,slack_env,host_url)), job, LoggingLevel.INFO, true).tapError(ex => UIO(logger.error(ex.getMessage)))
          } yield cleanSlackMessage(slack.final_message)

        assertM(slackInfoLevelExecutor)(equalTo(message))
      },
      testM("Execute job with log level DEBUG - Success case") {

        val jri = java.util.UUID.randomUUID.toString
        val output_host_url = s"localhost:8080/JobRunDetails/$jri"

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
                                                |          *Details Available at*: $output_host_url
                                                |          *Steps (Task - Duration)*:
                                                | :small_blue_diamond:*ProcessData* - (x.xx secs)
                                                |
                                                | :small_blue_diamond:*ProcessData* - (x.xx secs)
                                                |""".stripMargin)

        val slackMessageResult =
          for {
            slack <- JobExecutor(job_name, jri, Some(Slack(slack_url,slack_env,host_url)), job, LoggingLevel.DEBUG, true).tapError(ex => UIO(logger.error(ex.getMessage)))
          } yield cleanSlackMessage(slack.final_message)

        assertM(slackMessageResult)(equalTo(debugMessage))
      },
      testM("Execute job with log level DEBUG - Failure case") {

        val jri = java.util.UUID.randomUUID.toString
        val output_host_url = s"localhost:8080/JobRunDetails/$jri"

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
                                                       |          *Details Available at*: $output_host_url
                                                       |          *Steps (Task - Duration)*:
                                                       | :small_orange_diamond:*ProcessData* - (x.xx secs)
                                                       |			  error -> Failed in processing data""".stripMargin)

        val slackDebugLevelExecutor =
          for {
            slack <- JobExecutor(job_name, jri, Some(Slack(slack_url,slack_env,host_url)), job, LoggingLevel.DEBUG, true).tapError(ex => UIO(logger.error(ex.getMessage)))
          } yield cleanSlackMessage(slack.final_message)

        assertM(slackDebugLevelExecutor)(equalTo(debugFailureMessage))
      },
      testM("Execute job with log level JOB - Success Case") {

        val jri = java.util.UUID.randomUUID.toString
        val output_host_url = s"localhost:8080/JobRunDetails/$jri"

        val step1 = GenericETLStep(
          name               = "ProcessData",
          transform_function = processData,
        )

        val job = step1.execute()

        val message = cleanSlackMessage(s""":large_blue_circle: dev-testing - EtlSlackJob Process *Success!*
                                           |          *Time of Execution*: xxxx-xx-xx xx:xx:xx$tz
                                           |          *Details Available at*: $output_host_url
                                           |          *Steps (Task - Duration)*:
                                           | :small_blue_diamond:*ProcessData* - (x.xx secs)""".stripMargin)


        val slackJobLevelExecutor =
          for {
            slack <- JobExecutor(job_name, jri, Some(Slack(slack_url,slack_env,host_url)), job, LoggingLevel.JOB, true).tapError(ex => UIO(logger.error(ex.getMessage)))
          } yield cleanSlackMessage(slack.final_message)

        assertM(slackJobLevelExecutor)(equalTo(message))
      },
      testM("Execute job with log level JOB - Failure Case") {

        val jri = java.util.UUID.randomUUID.toString
        val output_host_url = s"localhost:8080/JobRunDetails/$jri"

        val step1 = GenericETLStep(
          name               = "ProcessData",
          transform_function = processDataFail,
        )

        val job = step1.execute()

        val message = cleanSlackMessage(s""":red_circle: dev-testing - EtlSlackJob Process *Failed!*
                                           |          *Time of Execution*: xxxx-xx-xx xx:xx:xx$tz
                                           |          *Details Available at*: $output_host_url
                                           |          *Steps (Task - Duration)*:
                                           |                      :small_orange_diamond:*ProcessData* - (x.xx secs)
                                           |			                  error -> Failed in processing data""".stripMargin)

        val slackJobLevelExecutor =
          for {
            slack <- JobExecutor(job_name, jri, Some(Slack(slack_url,slack_env,host_url)), job, LoggingLevel.JOB, true).tapError(ex => UIO(logger.error(ex.getMessage)))
          } yield cleanSlackMessage(slack.final_message)

        assertM(slackJobLevelExecutor)(equalTo(message))
      }
    ) @@ TestAspect.sequential).provideCustomLayerShared((env).orDie)
}