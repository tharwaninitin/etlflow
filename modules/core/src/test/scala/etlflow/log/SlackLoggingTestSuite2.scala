package etlflow.log

import etlflow.Schema.Rating
import etlflow.etlsteps.{GenericETLStep, SparkReadStep}
import etlflow.log.EtlLogger.JobLogger
import etlflow.spark.SparkManager
import etlflow.utils.{LoggingLevel, PARQUET, UtilityFunctions => UF}
import etlflow.{EtlJobProps, LoggerResource, TestSuiteHelper}
import org.apache.spark.sql.SparkSession
import org.slf4j.{Logger, LoggerFactory}
import zio.test.Assertion.equalTo
import zio.test._
import zio.{Task, UIO, ZIO}

object SlackLoggingTestSuite2 extends DefaultRunnableSpec  with TestSuiteHelper{

  final val etl_job_logger: Logger = LoggerFactory.getLogger(getClass.getName)
  val slack_url = ""
  val env = "dev-testing"
  val job_name = "EtlSlackJob"
  private implicit val spark: SparkSession = SparkManager.createSparkSession()

  def spec: ZSpec[environment.TestEnvironment, Any] =
  suite("EtlFlow Steps") (
    testM("Execute EtlFlowJobStep for log level Debug - Success case") {
      case class EtlJobSlackProps (
                                    override val job_notification_level: LoggingLevel = LoggingLevel.DEBUG,
                                  ) extends EtlJobProps

      val slackProps = EtlJobSlackProps()

      def processData(ip: Unit): Unit = {
        etl_job_logger.info("Hello World")
      }

      val step1 = GenericETLStep(
        name               = "ProcessData",
        transform_function = processData,
      )

      val step2 = SparkReadStep[Rating](
        name             = "GetRatingsParquet",
        input_location   = Seq(s"$canonical_path/modules/core/src/test/resources/input/movies/ratings_parquet/ratings.parquet"),
        input_type       = PARQUET,
      )


      val job: ZIO[LoggerResource, Throwable, Unit] =
        for {
          _   <- step1.execute()
          _   <- step2.execute()
        } yield ()


      val debugMessage = """:large_blue_circle: dev-testing - EtlSlackJob Process *Success!*
                           |          *Time of Execution*: xxxx-xx-xx xx:xx:xx
                           |          *Steps (Task - Duration)*:
                           | :small_blue_diamond:*ProcessData* - (x.xx secs)
                           |
                           | :small_blue_diamond:*GetRatingsParquet* - (x.xx secs)
                           |			 input_location -> /Users/sonawanes/Desktop/nitin/etlflow-fork-xxxxxx/etlflow/modules/core/src/test/resources/input/movies/ratings_parquet/ratings.parquet, input_type -> PARQUET, input_class -> `user_id` INT,`movie_id` INT,`rating` DOUBLE,`timestamp` BIGINT
                           |""".stripMargin.replaceAll("\\s+","")

      val slackDebugLevelExecuter =
        for {
          slack      <- Task(SlackLogManager.createSlackLogger(job_name, slackProps,env,slack_url))
          resource   =  LoggerResource(None,Some(slack))
          job_start_time  <- UIO.succeed(UF.getCurrentTimestamp)
          log        =  JobLogger.live(resource)
          _          <- job.provide(resource).foldM(
            ex => log.logError(job_start_time,ex),
            _  => log.logSuccess(job_start_time)
          )
          value      <- Task(slack.final_message.replaceAll("[0-9]", "x").replaceAll("\\s+","").equals(debugMessage))
          _          = println("success slack.final_message :"  + slack.final_message.replaceAll("[0-9]", "x"))
        } yield (value)

      assertM(slackDebugLevelExecuter)(equalTo(true))
    },
    testM("Execute EtlFlowJobStep for log level Debug - Failure case") {
      case class EtlJobSlackProps (
                                    override val job_notification_level: LoggingLevel = LoggingLevel.DEBUG,
                                  ) extends EtlJobProps

      val slackProps = EtlJobSlackProps()

      def processData(ip: Unit): Unit = {
        etl_job_logger.info("Hello World")
        throw new RuntimeException("Failed in processing data")
      }

      val step1 = GenericETLStep(
        name               = "ProcessData",
        transform_function = processData,
      )

      val step2 = SparkReadStep[Rating](
        name             = "GetRatingsParquet",
        input_location   = Seq(s"$canonical_path/modules/core/src/test/resources/input/movies/ratings_parquet/ratings.parquet"),
        input_type       = PARQUET,
      )


      val job1: ZIO[LoggerResource, Throwable, Unit] =
        for {
          _   <- step1.execute().foldM(
            ex => ZIO.succeed(),
            _ => ZIO.succeed()
          )
          _   <- step2.execute()
        } yield ()


      val debugFailureMessage = """:large_blue_circle: dev-testing - EtlSlackJob Process *Success!*
                                  |          *Time of Execution*: xxxx-xx-xx xx:xx:xx
                                  |          *Steps (Task - Duration)*:
                                  | :small_orange_diamond:*ProcessData* - (x.xx secs)
                                  |			 , error -> Failed in processing data
                                  | :small_blue_diamond:*GetRatingsParquet* - (x.xx secs)
                                  |			 input_location -> /Users/sonawanes/Desktop/nitin/etlflow-fork-xxxxxx/etlflow/modules/core/src/test/resources/input/movies/ratings_parquet/ratings.parquet, input_type -> PARQUET, input_class -> `user_id` INT,`movie_id` INT,`rating` DOUBLE,`timestamp` BIGINT
                                  |""".stripMargin.replaceAll("\\s+","")

      val slackDebugLevelExecuter =
        for {
          slack      <- Task(SlackLogManager.createSlackLogger(job_name, slackProps,env,slack_url))
          resource   =  LoggerResource(None,Some(slack))
          job_start_time  <- UIO.succeed(UF.getCurrentTimestamp)
          log        =  JobLogger.live(resource)
          _          <- job1.provide(resource).foldM(
            ex => log.logError(job_start_time,ex),
            _  => log.logSuccess(job_start_time)
          )
          value      <- Task(slack.final_message.replaceAll("[0-9]", "x").replaceAll("\\s+","").equals(debugFailureMessage))
          _          = println("failure slack.final_message :"  + slack.final_message.replaceAll("[0-9]", "x"))
        } yield (value)

      assertM(slackDebugLevelExecuter)(equalTo(true))
    },
  )
}
