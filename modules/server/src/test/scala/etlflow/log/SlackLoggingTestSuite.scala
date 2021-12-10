package etlflow.log

import etlflow.core.CoreEnv
import etlflow.etlsteps.GenericETLStep
import etlflow.log
import etlflow.schema.Slack
import etlflow.utils.ApplicationLogger
import zio.UIO
import zio.test.Assertion.equalTo
import zio.test._
import java.util.TimeZone

object SlackLoggingTestSuite extends ApplicationLogger {

  private val tz = TimeZone.getDefault.getDisplayName(false, TimeZone.SHORT)

  val slack_url = ""
  val slack_env = "dev-testing"
  val host_url = "localhost:8080"

  val job_name = "EtlSlackJob"

  def processData(ip: Unit): Unit = {
    logger.info("Hello World")
  }

  def processDataFail(ip: Unit): Unit = {
    logger.info("Hello World :" + ip)
    throw new RuntimeException("Failed in processing data")
  }

  def cleanSlackMessage(msg: String): String = {
    msg.replaceAll("[0-9]", "x").replaceAll("\\s+", "").replaceAll("x", "")
  }

  val step2 = GenericETLStep(
    name = "ProcessData",
    transform_function = processData,
  )

  def slackLayer = SlackImplementation.live(Some(Slack(slack_url,slack_env,host_url)))

  val spec: ZSpec[environment.TestEnvironment with CoreEnv, Any] =
    suite("EtlFlow Slack Logger")(
      testM("Execute job with Success Case") {

        val step1 = GenericETLStep(
          name = "ProcessData",
          transform_function = processData,
        )

        val jri = java.util.UUID.randomUUID.toString
        val output_host_url = s"localhost:8080/JobRunDetails/$jri"

        val job = step1.execute(())

        val message = cleanSlackMessage(
          f"""
              :large_blue_circle: dev-testing - EtlSlackJob Process *Success!*
                *Time of Execution*: xxxx-xx-xx xx:xx:xx$tz
                *Details Available at*: $output_host_url
                *Steps (Task - Duration)*:
                  :small_blue_diamond:*ProcessData* - (x.xx secs)
              """.stripMargin)

        val slackInfoLevelExecutor = JobExecutor(job_name, jri, job)
          .tapError(ex => UIO(logger.error(ex.getMessage)))
          .flatMap(_ => log.SlackApi.getSlackNotification)
          .provideSomeLayer[CoreEnv](slackLayer)
          .map(cleanSlackMessage)

        assertM(slackInfoLevelExecutor)(equalTo(message))
      },
      testM("Execute job with Failure Case") {

        val jri = java.util.UUID.randomUUID.toString
        val output_host_url = s"localhost:8080/JobRunDetails/$jri"

        val step1 = GenericETLStep(
          name               = "ProcessData",
          transform_function = processDataFail,
        )

        val job = step1.execute(())

        val message = cleanSlackMessage(f"""
                    :red_circle: dev-testing - EtlSlackJob Process *Failed!*
                    *Time of Execution*: xxxx-xx-xx xx:xx:xx$tz
                    *Details Available at*: $output_host_url
                    *Steps (Task - Duration)*:
                      :small_orange_diamond:*ProcessData* - (x.xx secs)
			                  error -> Failed in processing data
                    """.stripMargin)

        val slackInfoLevelExecutor = JobExecutor(job_name, jri, job)
          .tapError(ex => UIO(logger.error(ex.getMessage)))
          .flatMap(_ => log.SlackApi.getSlackNotification)
          .provideSomeLayer[CoreEnv](slackLayer)
          .map(cleanSlackMessage)

        assertM(slackInfoLevelExecutor)(equalTo(message))
      }
    ) @@ TestAspect.sequential
}