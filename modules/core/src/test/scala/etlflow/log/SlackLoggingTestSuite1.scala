package etlflow.log

import etlflow.etlsteps.GenericETLStep
import etlflow.log.EtlLogger.JobLogger
import etlflow.spark.SparkManager
import etlflow.utils.{LoggingLevel, UtilityFunctions => UF}
import etlflow.{EtlJobProps, LoggerResource, TestSuiteHelper}
import org.apache.spark.sql.SparkSession
import org.slf4j.{Logger, LoggerFactory}
import zio.test.Assertion.equalTo
import zio.test._
import zio.{Task, UIO, ZIO}

object SlackLoggingTestSuite1 extends DefaultRunnableSpec  with TestSuiteHelper{

  final val etl_job_logger: Logger = LoggerFactory.getLogger(getClass.getName)
  val slack_url = ""
  val env = "dev-testing"
  val job_name = "EtlSlackJob"
  private implicit val spark: SparkSession = SparkManager.createSparkSession()

  def spec: ZSpec[environment.TestEnvironment, Any] =
    suite("EtlFlow Steps") (
      testM("Execute EtlFlowJobStep for log level Info - Success Case") {
        case class EtlJobSlackProps (
                                      override val job_notification_level: LoggingLevel = LoggingLevel.INFO,
                                    ) extends EtlJobProps

        val slackProps = EtlJobSlackProps()

        def processData(ip: Unit): Unit = {
          etl_job_logger.info("Hello World")
        }

        val step1 = GenericETLStep(
          name               = "ProcessData",
          transform_function = processData,
        )

        val job: ZIO[LoggerResource, Throwable, Unit] =
          for {
            _   <- step1.execute()
          } yield ()

        val message = f"""
              :large_blue_circle: dev-testing - EtlSlackJob Process *Success!*
                *Time of Execution*: xxxx-xx-xx xx:xx:xx
                *Steps (Task - Duration)*:
                  :small_blue_diamond:*ProcessData* - (x.xx secs)
              """.stripMargin.replaceAll("\\s+","")

        val slackInfoLevelExecuter =
          for {
            slack      <- Task(SlackLogManager.createSlackLogger(job_name, slackProps,env,slack_url))
            resource   =  LoggerResource(None,Some(slack))
            job_start_time  <- UIO.succeed(UF.getCurrentTimestamp)
            log        =  JobLogger.live(resource)
            _          <- job.provide(resource).foldM(
                            ex => log.logError(job_start_time,ex),
                            _  => log.logSuccess(job_start_time)
                        )
            value      <- Task(slack.final_message.replaceAll("[0-9]", "x").replaceAll("\\s+","").equals(message))
          } yield (value)

        assertM(slackInfoLevelExecuter)(equalTo(true))
      },
      testM("Execute EtlFlowJobStep for log level Info - Failure Case") {
        case class EtlJobSlackProps (
                                      override val job_notification_level: LoggingLevel = LoggingLevel.INFO,
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

        val job: ZIO[LoggerResource, Throwable, Unit] =
          for {
            _   <- step1.execute().foldM(
              ex => ZIO.succeed(),
               _ => ZIO.succeed()
              )
          } yield ()

        val message = f"""
                    :large_blue_circle: dev-testing - EtlSlackJob Process *Success!*
                    *Time of Execution*: xxxx-xx-xx xx:xx:xx
                    *Steps (Task - Duration)*:
                      :small_orange_diamond:*ProcessData* - (x.xx secs)
			                  error -> Failed in processing data
                    """.stripMargin.replaceAll("\\s+","")

        val slackInfoLevelExecuter =
          for {
            slack      <- Task(SlackLogManager.createSlackLogger(job_name, slackProps,env,slack_url))
            resource   =  LoggerResource(None,Some(slack))
            job_start_time  <- UIO.succeed(UF.getCurrentTimestamp)
            log        =  JobLogger.live(resource)
            _          <- job.provide(resource).foldM(
              ex => log.logError(job_start_time,ex),
              _  => log.logSuccess(job_start_time)
            )
            value      <- Task(slack.final_message.replaceAll("[0-9]", "x").replaceAll("\\s+","").equals(message))
            _          = println("message :" + slack.final_message.replaceAll("[0-9]", "x"))
          } yield (value)

        assertM(slackInfoLevelExecuter)(equalTo(true))
      },
      testM("Execute EtlFlowJobStep for log level Job - Success case") {

        case class EtlJobSlackProps (
                                      override val job_notification_level: LoggingLevel = LoggingLevel.JOB,
                                    ) extends EtlJobProps

        val slackProps = EtlJobSlackProps()

        def processData(ip: Unit): Unit = {
          etl_job_logger.info("Hello World")
        }

        val step1 = GenericETLStep(
          name               = "ProcessData",
          transform_function = processData,
        )

        val job: ZIO[LoggerResource, Throwable, Unit] =
          for {
            _   <- step1.execute()
          } yield ()


        val message = """:large_blue_circle: dev-testing - EtlSlackJob Process *Success!*
                        |          *Time of Execution*: xxxx-xx-xx xx:xx:xx""".stripMargin.replaceAll("\\s+","")

        val slackJobLevelExecuter =
          for {
            slack      <- Task(SlackLogManager.createSlackLogger(job_name, slackProps,env,slack_url))
            resource   =  LoggerResource(None,Some(slack))
            job_start_time  <- UIO.succeed(UF.getCurrentTimestamp)
            log        =  JobLogger.live(resource)
            _          <- job.provide(resource).foldM(
              ex => log.logError(job_start_time,ex),
              _  => log.logSuccess(job_start_time)
            )
            value      <- Task(slack.final_message.replaceAll("[0-9]", "x").replaceAll("\\s+","").equals(message))
            _          = println("slack.final_message :"  + slack.final_message.replaceAll("[0-9]", "x"))
          } yield (value)

        assertM(slackJobLevelExecuter)(equalTo(true))
      },
      testM("Execute EtlFlowJobStep for log level Job - Failure case") {

        case class EtlJobSlackProps (
                                      override val job_notification_level: LoggingLevel = LoggingLevel.JOB,
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

        val job: ZIO[LoggerResource, Throwable, Unit] =
          for {
            _   <- step1.execute().foldM(
              ex => ZIO.succeed(),
              _ => ZIO.succeed()
            )
          } yield ()


        val message = """:large_blue_circle: dev-testing - EtlSlackJob Process *Success!*
                        |          *Time of Execution*: xxxx-xx-xx xx:xx:xx""".stripMargin.replaceAll("\\s+","")

        val slackJobLevelExecuter =
          for {
            slack      <- Task(SlackLogManager.createSlackLogger(job_name, slackProps,env,slack_url))
            resource   =  LoggerResource(None,Some(slack))
            job_start_time  <- UIO.succeed(UF.getCurrentTimestamp)
            log        =  JobLogger.live(resource)
            _          <- job.provide(resource).foldM(
              ex => log.logError(job_start_time,ex),
              _  => log.logSuccess(job_start_time)
            )
            value      <- Task(slack.final_message.replaceAll("[0-9]", "x").replaceAll("\\s+","").equals(message))
            _          = println("slack.final_message :"  + slack.final_message.replaceAll("[0-9]", "x"))
          } yield (value)

        assertM(slackJobLevelExecuter)(equalTo(true))
      }
    )
}
