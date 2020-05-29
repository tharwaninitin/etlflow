package etlflow.scheduler

import java.util.UUID.randomUUID

import caliban.CalibanError.ExecutionError
import ch.qos.logback.classic.{Level, Logger => LBLogger}
import cron4s.Cron
import doobie.hikari.HikariTransactor
import doobie.implicits._
import doobie.quill.DoobieContext
import etlflow.etljobs.{EtlJob => EtlFlowEtlJob}
import etlflow.log.JobRun
import etlflow.scheduler.EtlFlowHelper._
import etlflow.utils.{GlobalProperties, JDBC, UtilityFunctions => UF}
import etlflow.{EtlJobName, EtlJobProps}
import eu.timepit.fs2cron.schedule
import fs2.Stream
import io.getquill.Literal
import org.slf4j.{Logger, LoggerFactory}
import pdi.jwt.{Jwt, JwtAlgorithm}
import zio._
import zio.interop.catz._
import zio.interop.catz.implicits._
import zio.stream.ZStream

import scala.reflect.runtime.universe.TypeTag

abstract class SchedulerApp[EJN <: EtlJobName[EJP] : TypeTag, EJP <: EtlJobProps : TypeTag, EJGP <: GlobalProperties : TypeTag] extends GQLServerHttp4s {
  lazy val root_logger: LBLogger = LoggerFactory.getLogger("org.apache.spark").asInstanceOf[LBLogger]
  root_logger.setLevel(Level.WARN)

  def globalProperties: Option[EJGP]
  val etl_job_name_package: String
  def toEtlJob(job_name: EJN): (EJP,Option[EJGP]) => EtlFlowEtlJob

  private lazy val global_properties: Option[EJGP] = globalProperties
  val DB_DRIVER: String = global_properties.map(x => x.log_db_driver).getOrElse("<not_set>")
  val DB_URL: String  = global_properties.map(x => x.log_db_url).getOrElse("<not_set>")     // connect URL
  val DB_USER: String = global_properties.map(x => x.log_db_user).getOrElse("<not_set>")    // username
  val DB_PASS: String = global_properties.map(x => x.log_db_pwd).getOrElse("<not_set>")    // password
  val credentials: JDBC = JDBC(DB_URL,DB_USER,DB_PASS,DB_DRIVER)

  object EtlFlowService {
    // DB Objects
    case class UserInfo(user_name: String, password: String, user_active: String)
    case class UserAuthTokens(token: String)
    case class CronJobDB(job_name: String, schedule: String)

    val logger: Logger = LoggerFactory.getLogger(getClass.getName)
    val dc = new DoobieContext.Postgres(Literal)
    import dc._

    val liveHttp4s: Layer[Throwable, EtlFlowHas] = ZLayer.fromEffect{
      for {
        _             <- runDbMigration(credentials)
        subscribers   <- Ref.make(List.empty[Queue[EtlJobStatus]])
        activeJobs    <- Ref.make(0)
        cronJobs      <- Ref.make(List.empty[CronJob])
        transactor    <- createDbTransactorJDBC(credentials, platform.executor.asEC, "EtlFlowScheduler-Pool")
        _             <- Task(logger.info(s"Getting CRON JOBs from DATABASE"))
        dbCronJobs    <- getCronJobsDB(transactor)
        _             <- cronJobs.update{_ => dbCronJobs}
        _             <- Task(logger.info(s"Starting CRON JOB SCHEDULING \n${dbCronJobs.mkString("\n")}"))
        _             <- scheduledTask(dbCronJobs).fork
      } yield new EtlFlow.Service {

        override def runJob(args: EtlJobArgs): ZIO[EtlFlowHas, Throwable, EtlJob] = {

          val etlJobDetails: Task[(EJN, EtlFlowEtlJob, Map[String, String])] = Task {
            val job_name      = UF.getEtlJobName[EJN](args.name, etl_job_name_package)
            val props_map     = args.props.map(x => (x.key,x.value)).toMap
            val etl_job       = toEtlJob(job_name)(job_name.getActualProperties(props_map),globalProperties)
            etl_job.job_name  = job_name.toString
            (job_name,etl_job,props_map)
          }.mapError(e => ExecutionError(e.getMessage))

          val job = for {
            queues                       <- subscribers.get
            (job_name,etl_job,props_map) <- etlJobDetails
            execution_props <- Task {
              UF.convertToJsonByRemovingKeysAsMap(job_name.getActualProperties(props_map), List.empty)
                .map(x => (x._1, x._2.toString))
            }.mapError{ e =>
              logger.error(e.getMessage)
              ExecutionError(e.getMessage)
            }
            _               <- activeJobs.update(_ + 1)
            _               <- UIO.foreach(queues){queue =>
              queue
                .offer(EtlJobStatus(etl_job.job_name,"Started",execution_props))
                .catchSomeCause {
                  case cause if cause.interrupted =>
                    subscribers.update(_.filterNot(_ == queue)).as(false)
                } // if queue was shutdown, remove from subscribers
            }
            _               <- etl_job.execute().ensuring{
              activeJobs.update(_ - 1) *>
                UIO.foreach(queues){queue =>
                  queue
                    .offer(EtlJobStatus(etl_job.job_name,"Completed",execution_props))
                    .catchSomeCause {
                      case cause if cause.interrupted =>
                        subscribers.update(_.filterNot(_ == queue)).as(false)
                    } // if queue was shutdown, remove from subscribers
                }
            }.forkDaemon
          } yield EtlJob(args.name,execution_props)
          job
        }

        override def getEtlJobs: ZIO[EtlFlowHas, Throwable, List[EtlJob]] = {
          Task{
            UF.getEtlJobs[EJN].map(x => EtlJob(x,getJobProps(x))).toList
          }.mapError{ e =>
            logger.error(e.getMessage)
            ExecutionError(e.getMessage)
          }
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
              val number = randomUUID().toString.split("-")(0)
              val token = Jwt.encode(s"""${args.user_name}:$number""", "secretKey", JwtAlgorithm.HS256)
              logger.info("Token generated " + token)
              val userAuthToken = quote {
                query[UserAuthTokens].insert(lift(UserAuthTokens(token)))
              }
              dc.run(userAuthToken).transact(transactor).map(z => UserAuth("Valid User", token))
            }
          })
        }

        override def getInfo: ZIO[EtlFlowHas, Throwable, EtlFlowMetrics] = {
          for {
            x <- activeJobs.get
            y <- subscribers.get
            z <- cronJobs.get
            a <- getEtlJobs
          } yield EtlFlowMetrics(x, y.length, a.length, z.length)
        }

        override def addCronJob(args: CronJob): ZIO[EtlFlowHas, Throwable, CronJob] = {
          val cronJobDB = CronJobDB(args.job_name, args.schedule.toString)
          val cronJobString = quote {
            querySchema[CronJobDB]("cronjob").insert(lift(cronJobDB))
          }
          dc.run(cronJobString).transact(transactor).map(_ => args)
          }.mapError { e =>
          logger.error(e.getMessage)
          ExecutionError(e.getMessage)
        }

        override def updateCronJob(args: CronJob): ZIO[EtlFlowHas, Throwable, CronJob] = {
          val cronJobStringUpdate = quote {
            querySchema[CronJobDB]("cronjob")
              .filter(x => x.job_name == lift(args.job_name))
              .update{
                _.schedule -> lift(args.schedule.toString)
              }
          }
          dc.run(cronJobStringUpdate).transact(transactor).map(_ => args)
          }.mapError { e =>
          logger.error(e.getMessage)
          ExecutionError(e.getMessage)
        }

        override def getCronJobs: ZIO[EtlFlowHas, Throwable, List[CronJob]] = {
          getCronJobsDB(transactor)
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

        override def getStream: ZStream[Any, Nothing, EtlFlowMetrics] = ZStream(EtlFlowMetrics(1,1,1,1))

        def getLogs: ZIO[EtlFlowHas, Throwable, EtlFlowMetrics] = {
          // ZStream.fromIterable(Seq(EtlFlowInfo(1), EtlFlowInfo(2), EtlFlowInfo(3))).runCollect.map(x => x.head)
          ZStream.fromIterable(Seq(EtlFlowMetrics(1,1,1,1), EtlFlowMetrics(2,2,2,2), EtlFlowMetrics(3,3,3,3))).runCollect.map(x => x.head)
          // Command("echo", "-n", "1\n2\n3").linesStream.runCollect.map(x => EtlFlowInfo(x.head.length))
        }
      }
    }

    def getCronJobsDB(transactor: HikariTransactor[Task]): Task[List[CronJob]] = {
      val query = quote {
        querySchema[CronJobDB]("cronjob")
      }
      dc.run(query).transact(transactor).map(x => x.map(x => CronJob(x.job_name, Cron.unsafeParse(x.schedule))))
    }

    def scheduledTask(dbCronJobs: List[CronJob]): Task[Unit] = {
      val cronSchedule = schedule(dbCronJobs.map(cj => (cj.schedule,Stream.eval(
          UIO(logger.info(s"Starting CRON JOB ${cj.job_name} with schedule ${cj.schedule.toString} at ${UF.getCurrentTimestampAsString()}")) *>
          triggerJob(cj)
      ))))
      cronSchedule.compile.drain
    }

    def triggerJob(cronJob: CronJob): Task[EtlJob] = {

      val etlJobDetails: Task[(EJN, EtlFlowEtlJob)] = Task {
        val job_name      = UF.getEtlJobName[EJN](cronJob.job_name, etl_job_name_package)
        val etl_job       = toEtlJob(job_name)(job_name.getActualProperties(Map.empty),globalProperties)
        etl_job.job_name  = job_name.toString
        (job_name,etl_job)
      }.mapError{ e =>
        logger.error(e.getMessage)
        ExecutionError(e.getMessage)
      }

      val job = for {
        (job_name,etl_job)  <- etlJobDetails
        execution_props     <- Task {
                                  UF.convertToJsonByRemovingKeysAsMap(job_name.getActualProperties(Map.empty), List.empty)
                                    .map(x => (x._1, x._2.toString))
                                }.mapError{ e =>
                                  logger.error(e.getMessage)
                                  ExecutionError(e.getMessage)
                                }
        _                   <- etl_job.execute().forkDaemon
      } yield EtlJob(cronJob.job_name,execution_props)
      job
    }

    def getJobProps(jobName: String): Map[String, String] = {
      val name = UF.getEtlJobName[EJN](jobName,etl_job_name_package)
      val exclude_keys = List("job_run_id","job_description","job_properties")
      UF.convertToJsonByRemovingKeysAsMap(name.default_properties,exclude_keys).map(x =>(x._1,x._2.toString))
    }
  }

  val etlFlowLayer: Layer[Throwable, EtlFlowHas] = EtlFlowService.liveHttp4s
}
