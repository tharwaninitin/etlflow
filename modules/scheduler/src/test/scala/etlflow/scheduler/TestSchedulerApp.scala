package etlflow.scheduler

import java.util.UUID.randomUUID

import caliban.CalibanError.ExecutionError
import cron4s.Cron
import doobie.implicits._
import doobie.quill.DoobieContext
import doobie.util.transactor.Transactor.Aux
import etlflow.log.{JobRun, StepRun}
import etlflow.scheduler.CacheHelper
import etlflow.scheduler.api.EtlFlowHelper
import etlflow.scheduler.api.EtlFlowHelper.Creds.AWS
import etlflow.scheduler.api.EtlFlowHelper.{Credentials, CronJob, EtlFlow, EtlFlowHas, EtlJobStatus, Job, UserAuth}
import etlflow.utils.JsonJackson
import io.getquill.Literal
import org.slf4j.{Logger, LoggerFactory}
import pdi.jwt.{Jwt, JwtAlgorithm}
import scalacache.Cache
import zio.interop.catz._
import zio.stream.ZStream
import zio.{Queue, Ref, Task, ZIO, ZLayer}

trait TestSchedulerApp {
  lazy val logger: Logger = LoggerFactory.getLogger(getClass.getName)

  val dc = new DoobieContext.Postgres(Literal)
  import dc._

  case class UserInfo(user_name: String, password: String, user_active: String)
  case class CronJobDB(job_name: String, schedule: String, failed: Long, success: Long, is_active: Boolean)
  case class CredentialDB(name: String, `type`: String, value: String)

  def testHttp4s(transactor: Aux[Task, Unit], cache: Cache[String]) : ZLayer[Any, Throwable, EtlFlowHas] = ZLayer.fromEffect {
    for {
      subscribers       <- Ref.make(List.empty[Queue[EtlJobStatus]])
      activeJobs        <- Ref.make(0)
    } yield new EtlFlow.Service() {
      override def runJob(args: EtlFlowHelper.EtlJobArgs): ZIO[EtlFlowHas, Throwable, EtlFlowHelper.EtlJob] = ???

      override def updateJobState(args: EtlFlowHelper.EtlJobStateArgs): ZIO[EtlFlowHas, Throwable, Boolean] = {
        val cronJobStringUpdate = quote {
          querySchema[CronJobDB]("cronjob")
            .filter(x => x.job_name == lift(args.name))
            .update{
              _.is_active -> lift(args.state)
            }
        }
        dc.run(cronJobStringUpdate).transact(transactor).map(_ => args.state)
      }.mapError { e =>
        logger.error(e.getMessage)
        ExecutionError(e.getMessage)
      }

      override def login(args: EtlFlowHelper.UserArgs): ZIO[EtlFlowHas, Throwable, EtlFlowHelper.UserAuth] = {
        val q = quote {
          query[UserInfo].filter(x => x.user_name == lift(args.user_name) && x.password == lift(args.password))
        }
        dc.run(q).transact(transactor).flatMap(z => {
          if (z.isEmpty) {
            Task(UserAuth("Invalid User", ""))
          }
          else {
            Task {
              val number = randomUUID().toString.split("-")(0)
              val token = Jwt.encode(s"""${args.user_name}:$number""", "secretKey", JwtAlgorithm.HS256)
              logger.info("Token generated " + token)
              //val userAuthToken = quote {
              //  query[UserAuthTokens].insert(lift(UserAuthTokens(token)))
              //}
              //dc.run(userAuthToken).transact(transactor).map(z => UserAuth("Valid User", token))
              CacheHelper.putKey(cache,token,token)
              UserAuth("Valid User", token)
            }
          }
        })
      }

      override def addCronJob(args: EtlFlowHelper.CronJobArgs): ZIO[EtlFlowHas, Throwable, EtlFlowHelper.CronJob] = {
        val cronJobDB = CronJobDB(args.job_name, args.schedule.toString,0,0,true)
        val cronJobString = quote {
          querySchema[CronJobDB]("cronjob").insert(lift(cronJobDB))
        }
        dc.run(cronJobString).transact(transactor).map(_ => CronJob(cronJobDB.job_name,Cron(cronJobDB.schedule).toOption,0,0))
      }.mapError { e =>
        logger.error(e.getMessage)
        ExecutionError(e.getMessage)
      }

      override def updateCronJob(args: EtlFlowHelper.CronJobArgs): ZIO[EtlFlowHas, Throwable, EtlFlowHelper.CronJob] = {
        val cronJobStringUpdate = quote {
          querySchema[CronJobDB]("cronjob")
            .filter(x => x.job_name == lift(args.job_name))
            .update{
              _.schedule -> lift(args.schedule.toString)
            }
        }
        dc.run(cronJobStringUpdate).transact(transactor).map(_ => CronJob(args.job_name,Some(args.schedule),0,0))
      }.mapError { e =>
        logger.error(e.getMessage)
        ExecutionError(e.getMessage)
      }

      override def getInfo: ZIO[EtlFlowHas, Throwable, EtlFlowHelper.EtlFlowMetrics] = ???

      override def getJobs: ZIO[EtlFlowHas, Throwable, List[Job]] = {
        val selectQuery = quote {
          querySchema[CronJobDB]("cronjob")
        }
        dc.run(selectQuery)
          .transact(transactor)
          .map(y => y.map{x =>
            Job(x.job_name, Map.empty, Cron(x.schedule).toOption, x.failed, x.success, x.is_active)
          })
      }.mapError{ e =>
        logger.error(e.getMessage)
        ExecutionError(e.getMessage)
      }

      override def getDbJobRuns(args: EtlFlowHelper.DbJobRunArgs): ZIO[EtlFlowHas, Throwable, List[JobRun]] = {
        var q: Quoted[Query[JobRun]] = null
        try {
          if (args.jobRunId.isDefined && args.jobName.isEmpty) {
            q = quote {
              query[JobRun]
                .filter(_.job_run_id == lift(args.jobRunId.get))
                .sortBy(p => p.inserted_at)(Ord.desc)
                .drop(lift(args.offset))
                .take(lift(args.limit))
            }
            // logger.info(s"Query Fragment Generated for arguments $args is ")
          }
          else if (args.jobRunId.isEmpty && args.jobName.isDefined) {
            q = quote {
              query[JobRun]
                .filter(_.job_name == lift(args.jobName.get))
                .sortBy(p => p.inserted_at)(Ord.desc)
                .drop(lift(args.offset))
                .take(lift(args.limit))
            }
            // logger.info(s"Query Fragment Generated for arguments $args is " + q.toString)
          }
          else {
            q = quote {
              query[JobRun]
                .sortBy(p => p.inserted_at)(Ord.desc)
                .drop(lift(args.offset))
                .take(lift(args.limit))
            }
            // logger.info(s"Query Fragment Generated for arguments $args is " + q.toString)
          }
          dc.run(q).transact(transactor)
        }
        catch {
          case x: Throwable =>
            // This below code should be error free always
            logger.error(s"Exception occurred for arguments $args")
            x.getStackTrace.foreach(msg => logger.error("=> " + msg.toString))
            // Throw error here or DummyResults
            throw x
        }
      }

      override def getDbStepRuns(args: EtlFlowHelper.DbStepRunArgs): ZIO[EtlFlowHas, Throwable, List[StepRun]] = {
        try {
          val q = quote {
            query[StepRun]
              .filter(_.job_run_id == lift(args.job_run_id))
              .sortBy(p => p.inserted_at)(Ord.desc)
          }
          dc.run(q).transact(transactor)
        }
        catch {
          case x: Throwable =>
            logger.error(s"Exception occurred for arguments $args")
            x.getStackTrace.foreach(msg => logger.error("=> " + msg.toString))
            throw x
        }
      }

      override def notifications: ZStream[EtlFlowHas, Nothing, EtlFlowHelper.EtlJobStatus] = ???

      override def getStream: ZStream[EtlFlowHas, Nothing, EtlFlowHelper.EtlFlowMetrics] = ???

      override def addCredentials(args: EtlFlowHelper.CredentialsArgs): ZIO[EtlFlowHas, Throwable, EtlFlowHelper.Credentials] = {
        val credentialsDB = CredentialDB(
          args.name,
          args.`type`.get match {
            case etlflow.scheduler.api.EtlFlowHelper.Creds.JDBC => "jdbc"
            case AWS => "aws"
          },
          JsonJackson.convertToJsonByRemovingKeys(args.value.map(x => (x.key,x.value)).toMap, List.empty)
        )
        val credentialString = quote {
          querySchema[CredentialDB]("credentials").insert(lift(credentialsDB))
        }
        dc.run(credentialString).transact(transactor).map(_ => Credentials(credentialsDB.name,credentialsDB.`type`,credentialsDB.value))
      }.mapError { e =>
        logger.error(e.getMessage)
        ExecutionError(e.getMessage)
      }

      override def updateCredentials(args: EtlFlowHelper.CredentialsArgs): ZIO[EtlFlowHas, Throwable, EtlFlowHelper.Credentials] = {
        val value = JsonJackson.convertToJsonByRemovingKeys(args.value.map(x => (x.key,x.value)).toMap, List.empty)
        val cronJobStringUpdate = quote {
          querySchema[CredentialDB]("credentials")
            .filter(x => x.name == lift(args.name))
            .update{
              _.value -> lift(value)
            }
        }
        dc.run(cronJobStringUpdate).transact(transactor).map(_ => Credentials(args.name,"",value))
      }.mapError { e =>
        logger.error(e.getMessage)
        ExecutionError(e.getMessage)
      }
    }
  }

}
