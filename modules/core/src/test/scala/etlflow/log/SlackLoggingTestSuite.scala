package etlflow.log

import ch.qos.logback.classic.{Level, Logger => LBLogger}
import org.slf4j.{Logger, LoggerFactory}
import etlflow.{EtlJobProps, TestSparkSession}
import etlflow.Schema.Rating
import etlflow.etlsteps.{GenericETLStep, SparkReadStep}
import etlflow.spark.SparkManager
import etlflow.utils.{LoggingLevel, PARQUET}
import org.apache.spark.sql.SparkSession
import org.slf4j.{Logger, LoggerFactory}
import zio.test.Assertion.equalTo
import zio.test._

object SlackLoggingTestSuite extends DefaultRunnableSpec with TestSparkSession {

  lazy val logger: Logger = LoggerFactory.getLogger(getClass.getName)

  private val canonical_path = new java.io.File(".").getCanonicalPath

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
  def cleanSlackMessage(msg: String): String = {
    msg.replaceAll("[0-9]", "x").replaceAll("\\s+","").replaceAll("x","")
  }

  val step2 = SparkReadStep[Rating](
    name             = "GetRatingsParquet",
    input_location   = Seq(s"$canonical_path/modules/core/src/test/resources/input/movies/ratings_parquet/ratings.parquet"),
    input_type       = PARQUET,
  )

  def spec: ZSpec[environment.TestEnvironment, Any] =
  suite("EtlFlow Steps") (
    testM("Execute job with log level INFO - Success Case") {

      val slackProps = EtlJobSlackProps(LoggingLevel.INFO)

      val step1 = GenericETLStep(
        name               = "ProcessData",
        transform_function = processData,
      )

      val job = step1.execute()

      val message = cleanSlackMessage(f"""
              :large_blue_circle: dev-testing - EtlSlackJob Process *Success!*
                *Time of Execution*: xxxx-xx-xx xx:xx:xxIST
                *Steps (Task - Duration)*:
                  :small_blue_diamond:*ProcessData* - (x.xx secs)
              """.stripMargin)

      val slackInfoLevelExecutor =
        for {
          slack  <- JobExecutor.slack(job_name,slack_env,slack_url,slackProps,job)
        } yield cleanSlackMessage(slack.final_message)

      assertM(slackInfoLevelExecutor)(equalTo(message))
    },
    testM("Execute job with log level INFO - Failure Case") {

      val slackProps = EtlJobSlackProps(LoggingLevel.INFO)

      val step1 = GenericETLStep(
        name               = "ProcessData",
        transform_function = processDataFail,
      )

      val job = step1.execute()

      val message = cleanSlackMessage(f"""
                    :red_circle: dev-testing - EtlSlackJob Process *Failed!*
                    *Time of Execution*: xxxx-xx-xx xx:xx:xxIST
                    *Steps (Task - Duration)*:
                      :small_orange_diamond:*ProcessData* - (x.xx secs)
			                  error -> Failed in processing data
                    """.stripMargin)

      val slackInfoLevelExecutor =
        for {
          slack  <- JobExecutor.slack(job_name,slack_env,slack_url,slackProps,job)
        } yield cleanSlackMessage(slack.final_message)

      assertM(slackInfoLevelExecutor)(equalTo(message))
    },
    testM("Execute job with log level DEBUG - Success case") {

      val slackProps = EtlJobSlackProps(LoggingLevel.DEBUG)

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
                           |          *Time of Execution*: xxxx-xx-xx xx:xx:xxIST
                           |          *Steps (Task - Duration)*:
                           | :small_blue_diamond:*ProcessData* - (x.xx secs)
                           |
                           | :small_blue_diamond:*GetRatingsParquet* - (x.xx secs)
                           |			 input_location -> $canonical_path/modules/core/src/test/resources/input/movies/ratings_parquet/ratings.parquet, input_type -> PARQUET, input_class -> `user_id` INT,`movie_id` INT,`rating` DOUBLE,`timestamp` BIGINT
                           |""".stripMargin)

      val slackMessageResult =
        for {
          slack <- JobExecutor.slack(job_name,slack_env,slack_url,slackProps,job)
        } yield cleanSlackMessage(slack.final_message)

      assertM(slackMessageResult)(equalTo(debugMessage))
    },
    testM("Execute job with log level DEBUG - Failure case") {

      val slackProps = EtlJobSlackProps(LoggingLevel.DEBUG)

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
                                  |          *Time of Execution*: xxxx-xx-xx xx:xx:xxIST
                                  |          *Steps (Task - Duration)*:
                                  | :small_orange_diamond:*ProcessData* - (x.xx secs)
                                  |			 , error -> Failed in processing data""".stripMargin)

      val slackDebugLevelExecutor =
        for {
          slack <- JobExecutor.slack(job_name,slack_env,slack_url,slackProps,job)
        } yield cleanSlackMessage(slack.final_message)

      assertM(slackDebugLevelExecutor)(equalTo(debugFailureMessage))
    },
    testM("Execute job with log level JOB - Success Case") {

      val slackProps = EtlJobSlackProps(LoggingLevel.JOB)

      val step1 = GenericETLStep(
        name               = "ProcessData",
        transform_function = processData,
      )

      val job = step1.execute()

      val message = cleanSlackMessage(""":large_blue_circle: dev-testing - EtlSlackJob Process *Success!*
                      |          *Time of Execution*: xxxx-xx-xx xx:xx:xxIST""".stripMargin)

      val slackJobLevelExecutor =
        for {
          slack  <- JobExecutor.slack(job_name,slack_env,slack_url,slackProps,job)
        } yield cleanSlackMessage(slack.final_message)

      assertM(slackJobLevelExecutor)(equalTo(message))
    },
    testM("Execute job with log level JOB - Failure Case") {

      val slackProps = EtlJobSlackProps(LoggingLevel.JOB)

      val step1 = GenericETLStep(
        name               = "ProcessData",
        transform_function = processDataFail,
      )

      val job = step1.execute()

      val message = cleanSlackMessage(""":red_circle: dev-testing - EtlSlackJob Process *Failed!*
                      |          *Time of Execution*: xxxx-xx-xx xx:xx:xxIST
                      |          *Steps (Task - Duration)*:
                      |                      :small_orange_diamond:*ProcessData* - (x.xx secs)
                      |			                  error -> Failed in processing data""".stripMargin)

      val slackJobLevelExecutor =
        for {
          slack  <- JobExecutor.slack(job_name,slack_env,slack_url,slackProps,job)
        } yield cleanSlackMessage(slack.final_message)

      assertM(slackJobLevelExecutor)(equalTo(message))
    }
  ) @@ TestAspect.sequential
}
