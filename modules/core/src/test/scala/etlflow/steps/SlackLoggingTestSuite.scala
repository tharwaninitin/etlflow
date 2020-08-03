package etlflow.steps

import etlflow.etlsteps.GenericETLStep
import etlflow.log.SlackLogManager
import etlflow.utils.{LoggingLevel, UtilityFunctions => UF}
import etlflow.{EtlJobProps, LoggerResource, TestSuiteHelper}
import org.slf4j.{Logger, LoggerFactory}
import zio.test.Assertion._
import zio.test._
import zio.{Task, ZIO}


object EtlFlowSlackTestSuite extends DefaultRunnableSpec  with TestSuiteHelper{

  final val etl_job_logger: Logger = LoggerFactory.getLogger(getClass.getName)
  val slack_url = "<slack-url>"
  val env = "dev-testing"
  val job_name = "EtlSlackJob"

  def spec: ZSpec[environment.TestEnvironment, Any] =
    suite("EtlFlow Steps") (
      testM("Execute EtlFlowJobStep for log level Info") {
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


        val slackInfoLevelExecuter =
          for {
            slack      <- Task(SlackLogManager.createSlackLogger(job_name, slackProps,env,slack_url))
            resource   =  LoggerResource(None,Some(slack))
            _          <- job.provide(resource)
            value      <- Task(slack.final_message.contains("ProcessData"))
          } yield (value)

        assertM(slackInfoLevelExecuter)(equalTo(true))
      },
      testM("Execute EtlFlowJobStep for log level Debug") {
        case class EtlJobSlackProps (
                                      override val job_notification_level: LoggingLevel = LoggingLevel.DEBUG,
                                    ) extends EtlJobProps

        val slackProps1 = EtlJobSlackProps()

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


        val slackDebugLevelExecuter =
          for {
            slack      <- Task(SlackLogManager.createSlackLogger(job_name, slackProps1,env,slack_url))
            resource   =  LoggerResource(None,Some(slack))
            _          <- job.provide(resource)
            value      <- Task(slack.final_message.contains("ProcessData"))
          } yield (value)

        assertM(slackDebugLevelExecuter)(equalTo(true))
      },
      testM("Execute EtlFlowJobStep for log level Job") {

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


        val slackJobLevelExecuter =
          for {
            slack      <- Task(SlackLogManager.createSlackLogger(job_name, slackProps,env,slack_url))
            resource   =  LoggerResource(None,Some(slack))
            _          <- job.provide(resource)
            value      <- Task(slack.final_message.contains(""))
          } yield (value)

        assertM(slackJobLevelExecuter)(equalTo(true))
      },
      testM("Execute EtlFlowJobStep for log level Job (Negative Scenario)") {

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


        val slackJobLevelExecuter =
          for {
            slack      <- Task(SlackLogManager.createSlackLogger(job_name, slackProps,env,slack_url))
            resource   =  LoggerResource(None,Some(slack))
            _          <- job.provide(resource)
            value      <- Task(slack.final_message.contains("ProcessData"))
          } yield (value)

        assertM(slackJobLevelExecuter)(equalTo(false))
      },
      testM("Execute EtlFlowJobStep for Job level message template") {

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


        val execution_date_time = UF.getCurrentTimestampAsString("yyyy-MM-dd HH:mm:ss")

        val message =  f"""
          :large_blue_circle: $env - ${job_name} Process *Success!*
          *Time of Execution*: $execution_date_time
          """

        val slackJobLevelExecuter =
          for {
            slack      <- Task(SlackLogManager.createSlackLogger(job_name, slackProps,env,slack_url))
            resource   =  LoggerResource(None,Some(slack))
            _          <- job.provide(resource)
            value      <- Task(slack.finalMessageTemplate(env,execution_date_time,"","pass").equals(message))
          } yield (value)

        assertM(slackJobLevelExecuter)(equalTo(true))
      },
      testM("Execute EtlFlowJobStep for Info level message template") {

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


        val execution_date_time = UF.getCurrentTimestampAsString("yyyy-MM-dd HH:mm:ss")

        val message =  f"""
          :large_blue_circle: $env - ${job_name} Process *Success!*
          *Time of Execution*: $execution_date_time
          *Steps (Task - Duration)*:
          """

        val slackJobLevelExecuter =
          for {
            slack      <- Task(SlackLogManager.createSlackLogger(job_name, slackProps,env,slack_url))
            resource   =  LoggerResource(None,Some(slack))
            _          <- job.provide(resource)
            value      <- Task(slack.finalMessageTemplate(env,execution_date_time,"","pass").contains("Steps"))
          } yield (value)

        assertM(slackJobLevelExecuter)(equalTo(true))
      }
    )
}
