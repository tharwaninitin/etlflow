package etlflow.coretests.log

import etlflow.coretests.TestSuiteHelper
import etlflow.db.liveDBWithTransactor
import etlflow.etlsteps.GenericETLStep
import etlflow.schema.LoggingLevel
import zio.UIO
import zio.test.Assertion.equalTo
import zio.test._
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
    msg.replaceAll("[0-9]", "x").replaceAll("\\s+","").replaceAll("x","")
  }

  val step2 = GenericETLStep(
    name               = "ProcessData",
    transform_function = processData,
  )

  val env = liveDBWithTransactor(config.db) ++ jsonLayer ++ cryptoLayer  ++ loggerLayer

  def spec: ZSpec[environment.TestEnvironment, Any] =
    (suite("EtlFlow Slack Log Suite") (
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
            slack <- JobExecutor(job_name,config.slack,job,LoggingLevel.INFO,true).tapError(ex => UIO(logger.error(ex.getMessage)))
            _     = logger.info("slack :" + slack.final_message)
          } yield cleanSlackMessage(slack.final_message)

        assertM(slackInfoLevelExecutor)(equalTo(message))
      }
    ) @@ TestAspect.sequential).provideCustomLayerShared((env).orDie)
}
