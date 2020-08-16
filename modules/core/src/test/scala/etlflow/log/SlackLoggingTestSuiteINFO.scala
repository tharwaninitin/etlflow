package etlflow.log

import etlflow.{EtlJobProps, TestSuiteHelper}
import etlflow.etlsteps.GenericETLStep
import etlflow.utils.LoggingLevel
import zio.Task
import zio.test.Assertion.equalTo
import zio.test._

object SlackLoggingTestSuiteINFO extends DefaultRunnableSpec with TestSuiteHelper {

  val slack_url = ""
  val slack_env = "dev-testing"
  val job_name = "EtlSlackJob"

  case class EtlJobSlackProps(override val job_notification_level: LoggingLevel) extends EtlJobProps

  def processData(ip: Unit): Unit = {
    logger.info("Hello World")
  }
  def processDataFail(ip: Unit): Unit = {
    logger.info("Hello World")
    throw new RuntimeException("Failed in processing data")
  }

  def spec: ZSpec[environment.TestEnvironment, Any] =
    suite("EtlFlow Steps") (
      testM("Execute job with log level INFO - Success Case") {

        val slackProps = EtlJobSlackProps(LoggingLevel.INFO)

        val step1 = GenericETLStep(
          name               = "ProcessData",
          transform_function = processData,
        )

        val job = step1.execute()

        val message = f"""
              :large_blue_circle: dev-testing - EtlSlackJob Process *Success!*
                *Time of Execution*: xxxx-xx-xx xx:xx:xxIST
                *Steps (Task - Duration)*:
                  :small_blue_diamond:*ProcessData* - (x.xx secs)
              """.stripMargin.replaceAll("\\s+","")

        val slackInfoLevelExecutor =
          for {
            slack  <- JobExecutor.slack(job_name,slack_env,slack_url,slackProps,job)
            value  <- Task(slack.final_message.replaceAll("[0-9]", "x").replaceAll("\\s+","").equals(message))
          } yield value

        assertM(slackInfoLevelExecutor)(equalTo(true))
      },
      testM("Execute job with log level INFO - Failure Case") {

        val slackProps = EtlJobSlackProps(LoggingLevel.INFO)

        val step1 = GenericETLStep(
          name               = "ProcessData",
          transform_function = processDataFail,
        )

        val job = step1.execute()

        val message = f"""
                    :red_circle: dev-testing - EtlSlackJob Process *Failed!*
                    *Time of Execution*: xxxx-xx-xx xx:xx:xxIST
                    *Steps (Task - Duration)*:
                      :small_orange_diamond:*ProcessData* - (x.xx secs)
			                  error -> Failed in processing data
                    """.stripMargin.replaceAll("\\s+","")

        val slackInfoLevelExecutor =
          for {
            slack  <- JobExecutor.slack(job_name,slack_env,slack_url,slackProps,job)
            value  <- Task(slack.final_message.replaceAll("[0-9]", "x").replaceAll("\\s+","").equals(message))
          } yield value

        assertM(slackInfoLevelExecutor)(equalTo(true))
      },
    )
}
