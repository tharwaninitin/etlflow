package etlflow.scheduler.api

import java.util.UUID.randomUUID
import caliban.CalibanError.ExecutionError
import cron4s.Cron
import doobie.hikari.HikariTransactor
import doobie.implicits._
import doobie.quill.DoobieContext
import etlflow.{EtlJobName, EtlJobProps}
import etlflow.log.{JobRun, StepRun}
import etlflow.scheduler.CacheHelper
import etlflow.scheduler.api.EtlFlowHelper.Creds._
import etlflow.scheduler.api.EtlFlowHelper._
import etlflow.utils.Executor.{DATAPROC, LOCAL}
import etlflow.utils.{JsonJackson, UtilityFunctions => UF}
import io.getquill.Literal
import org.slf4j.{Logger, LoggerFactory}
import pdi.jwt.{Jwt, JwtAlgorithm}
import scalacache.Cache
import zio.{Queue, Ref, Task, UIO, ZIO, ZLayer}
import zio.blocking.Blocking
import zio.stream.ZStream
import zio.interop.catz._
import scala.reflect.runtime.universe.TypeTag

trait EtlFlowService {
  lazy val logger: Logger = LoggerFactory.getLogger(getClass.getName)

  val dc = new DoobieContext.Postgres(Literal)
  import dc._
  val javaRuntime: java.lang.Runtime = java.lang.Runtime.getRuntime
  val mb: Int = 1024*1024

  def runEtlJobRemote(args: EtlJobArgs, transactor: HikariTransactor[Task], config: DATAPROC): Task[EtlJob]
  def runEtlJobLocal(args: EtlJobArgs, transactor: HikariTransactor[Task]): Task[EtlJob]

  def liveHttp4s[EJN <: EtlJobName[EJP] : TypeTag, EJP <: EtlJobProps : TypeTag](
      transactor: HikariTransactor[Task],
      cache: Cache[String],
      cronJobs: Ref[List[CronJob]]
    ): ZLayer[Blocking, Throwable, EtlFlowHas] = ZLayer.fromEffect{
    for {
      subscribers       <- Ref.make(List.empty[Queue[EtlJobStatus]])
      activeJobs        <- Ref.make(0)
    } yield new EtlFlow.Service {

      val etl_job_name_package: String = UF.getJobNamePackage[EJN] + "$"

      private def getEtlJobs: Task[List[EtlJob]] = {
        Task{
          UF.getEtlJobs[EJN].map(x => EtlJob(x,getJobActualProps(x))).toList
        }.mapError{ e =>
          logger.error(e.getMessage)
          ExecutionError(e.getMessage)
        }
      }

      private def getJobActualProps(jobName: String): Map[String, String] = {
        val name = UF.getEtlJobName[EJN](jobName, etl_job_name_package)
        val exclude_keys = List("job_run_id","job_description","job_properties")
        JsonJackson.convertToJsonByRemovingKeysAsMap(name.getActualProperties(Map.empty), exclude_keys).map(x => (x._1, x._2.toString))
      }

      override def runJob(args: EtlJobArgs): ZIO[EtlFlowHas, Throwable, EtlJob] = {
        val job_deploy_mode = UF.getEtlJobName[EJN](args.name,etl_job_name_package).getActualProperties(Map.empty).job_deploy_mode
        job_deploy_mode match {
          case LOCAL =>
            logger.info("Running job in local mode ")
            runEtlJobLocal(args, transactor)
          case DATAPROC(project, region, endpoint, cluster_name) =>
            logger.info("Dataproc parameters are : " + project + "::" + region + "::"  + endpoint +"::" + cluster_name)
            runEtlJobRemote(args, transactor, DATAPROC(project, region, endpoint, cluster_name))
        }
      }

      override def updateJobState(args: EtlJobStateArgs): ZIO[EtlFlowHas, Throwable, Boolean] = {
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

      override def login(args: UserArgs): ZIO[EtlFlowHas, Throwable, UserAuth] =  {
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
              CacheHelper.putKey(cache,token,token)
              UserAuth("Valid User", token)
            }
          }
        })
      }

      override def getInfo: ZIO[EtlFlowHas, Throwable, EtlFlowMetrics] = {
        for {
          x <- activeJobs.get
          y <- subscribers.get
          z <- cronJobs.get
          a <- getEtlJobs
        } yield EtlFlowMetrics(
          x,
          y.length,
          a.length,
          z.length,
          used_memory = ((javaRuntime.totalMemory - javaRuntime.freeMemory) / mb).toString,
          free_memory = (javaRuntime.freeMemory / mb).toString,
          total_memory = (javaRuntime.totalMemory / mb).toString,
          max_memory = (javaRuntime.maxMemory / mb).toString,
          current_time = UF.getCurrentTimestampAsString()
        )
      }

      override def addCredentials(args: CredentialsArgs): ZIO[EtlFlowHas, Throwable, Credentials] = {
        val credentialsDB = CredentialDB(
          args.name,
          args.`type`.get match {
            case JDBC => "jdbc"
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

      override def updateCredentials(args: CredentialsArgs): ZIO[EtlFlowHas, Throwable, Credentials] = {
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

      override def addCronJob(args: CronJobArgs): ZIO[EtlFlowHas, Throwable, CronJob] = {
        val cronJobDB = CronJobDB(args.job_name, args.schedule.toString,0,0,is_active = true)
        val cronJobString = quote {
          querySchema[CronJobDB]("cronjob").insert(lift(cronJobDB))
        }
        dc.run(cronJobString).transact(transactor).map(_ => CronJob(cronJobDB.job_name,Cron(cronJobDB.schedule).toOption,0,0))
      }.mapError { e =>
        logger.error(e.getMessage)
        ExecutionError(e.getMessage)
      }

      override def updateCronJob(args: CronJobArgs): ZIO[EtlFlowHas, Throwable, CronJob] = {
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

      override def getDbStepRuns(args: DbStepRunArgs): ZIO[EtlFlowHas, Throwable, List[StepRun]] = {
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

      override def getDbJobRuns(args: DbJobRunArgs): ZIO[EtlFlowHas, Throwable, List[JobRun]] = {
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

      override def notifications: ZStream[EtlFlowHas, Nothing, EtlJobStatus] = ZStream.unwrap {
        for {
          queue <- Queue.unbounded[EtlJobStatus]
          _     <- UIO(logger.info(s"Starting new subscriber"))
          _     <- subscribers.update(queue :: _)
        } yield ZStream.fromQueue(queue).ensuring(queue.shutdown)
      }

      override def getStream: ZStream[Any, Nothing, EtlFlowMetrics] = ZStream(EtlFlowMetrics(1,1,1,1,"","","","",""))

      override def getJobs: ZIO[EtlFlowHas, Throwable, List[Job]] = {
        val selectQuery = quote {
          querySchema[CronJobDB]("cronjob")
        }
        dc.run(selectQuery)
          .transact(transactor)
          .map(y => y.map{x =>
            Job(x.job_name, getJobActualProps(x.job_name), Cron(x.schedule).toOption, x.failed, x.success, x.is_active)
          })
      }.mapError{ e =>
        logger.error(e.getMessage)
        ExecutionError(e.getMessage)
      }
    }
  }
}
