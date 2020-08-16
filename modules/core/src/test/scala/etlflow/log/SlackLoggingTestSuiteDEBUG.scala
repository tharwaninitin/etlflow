package etlflow.log

import etlflow.{EtlJobProps, TestSuiteHelper}
import etlflow.Schema.Rating
import etlflow.etlsteps.{GenericETLStep, SparkReadStep}
import etlflow.spark.SparkManager
import etlflow.utils.{LoggingLevel, PARQUET}
import org.apache.spark.sql.SparkSession
import zio.Task
import zio.test.Assertion.equalTo
import zio.test._

object SlackLoggingTestSuiteDEBUG extends DefaultRunnableSpec with TestSuiteHelper {

  private implicit val spark: SparkSession = SparkManager.createSparkSession()

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

  val step2 = SparkReadStep[Rating](
    name             = "GetRatingsParquet",
    input_location   = Seq(s"$canonical_path/modules/core/src/test/resources/input/movies/ratings_parquet/ratings.parquet"),
    input_type       = PARQUET,
  )

  def spec: ZSpec[environment.TestEnvironment, Any] =
  suite("EtlFlow Steps") (
    testM("Execute job for log level DEBUG - Success case") {

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

      val debugMessage = s""":large_blue_circle: dev-testing - EtlSlackJob Process *Success!*
                           |          *Time of Execution*: xxxx-xx-xx xx:xx:xxIST
                           |          *Steps (Task - Duration)*:
                           | :small_blue_diamond:*ProcessData* - (x.xx secs)
                           |
                           | :small_blue_diamond:*GetRatingsParquet* - (x.xx secs)
                           |			 input_location -> $canonical_path/modules/core/src/test/resources/input/movies/ratings_parquet/ratings.parquet, input_type -> PARQUET, input_class -> `user_id` INT,`movie_id` INT,`rating` DOUBLE,`timestamp` BIGINT
                           |""".stripMargin.replaceAll("\\s+","")

      val jobResult =
        for {
          slack  <- JobExecutor.slack(job_name,slack_env,slack_url,slackProps,job)
          value  <- Task(slack.final_message.replaceAll("[0-9]", "x").replaceAll("\\s+","").equals(debugMessage))
        } yield value

      assertM(jobResult)(equalTo(true))
    },
    testM("Execute job for log level DEBUG - Failure case") {

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


      val debugFailureMessage = s""":red_circle: dev-testing - EtlSlackJob Process *Failed!*
                                  |          *Time of Execution*: xxxx-xx-xx xx:xx:xxIST
                                  |          *Steps (Task - Duration)*:
                                  | :small_orange_diamond:*ProcessData* - (x.xx secs)
                                  |			 , error -> Failed in processing data""".stripMargin.replaceAll("\\s+","")

      val slackDebugLevelExecutor =
        for {
          slack  <- JobExecutor.slack(job_name,slack_env,slack_url,slackProps,job)
          value  <- Task(slack.final_message.replaceAll("[0-9]", "x").replaceAll("\\s+","").equals(debugFailureMessage))
        } yield value

      assertM(slackDebugLevelExecutor)(equalTo(true))
    },
  )
}
