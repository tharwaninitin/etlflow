package etlflow.scheduler

import java.util.UUID.randomUUID

import caliban.CalibanError.ExecutionError
import cron4s.Cron
import doobie.hikari.HikariTransactor
import doobie.implicits._
import doobie.quill.DoobieContext
import etlflow.log.JobRun
import etlflow.scheduler.EtlFlowHelper._
import etlflow.utils.{GlobalProperties, JDBC, UtilityFunctions => UF}
import etlflow.{EtlJobName, EtlJobProps}
import eu.timepit.fs2cron.schedule
import fs2.Stream
import io.getquill.Literal
import pdi.jwt.{Jwt, JwtAlgorithm}
import zio._
import zio.interop.catz._
import zio.interop.catz.implicits._
import zio.stream.ZStream

import scala.reflect.runtime.universe.TypeTag

abstract class SchedulerApp[EJN <: EtlJobName[EJP] : TypeTag, EJP <: EtlJobProps : TypeTag, EJGP <: GlobalProperties : TypeTag] extends GQLServerHttp4s {

  def globalProperties: Option[EJGP]
  val etl_job_name_package: String = UF.getJobNamePackage[EJN] + "$"

  lazy val global_properties: Option[EJGP] = globalProperties
  val DB_DRIVER: String = global_properties.map(x => x.log_db_driver).getOrElse("<not_set>")
  val DB_URL: String  = global_properties.map(x => x.log_db_url).getOrElse("<not_set>")     // connect URL
  val DB_USER: String = global_properties.map(x => x.log_db_user).getOrElse("<not_set>")    // username
  val DB_PASS: String = global_properties.map(x => x.log_db_pwd).getOrElse("<not_set>")    // password
  val credentials: JDBC = JDBC(DB_URL,DB_USER,DB_PASS,DB_DRIVER)

  // DB Objects
  case class UserInfo(user_name: String, password: String, user_active: String)
  case class UserAuthTokens(token: String)
  case class CronJobDB(job_name: String, schedule: String, failed: Long, success: Long)

  val dc = new DoobieContext.Postgres(Literal)
  import dc._

  object EtlFlowService {

    val liveHttp4s: Layer[Throwable, EtlFlowHas] = ZLayer.fromEffect{
      for {
        _             <- runDbMigration(credentials)
        subscribers   <- Ref.make(List.empty[Queue[EtlJobStatus]])
        activeJobs    <- Ref.make(0)
        cronJobs      <- Ref.make(List.empty[CronJob])
        transactor    <- createDbTransactorJDBC(credentials, platform.executor.asEC, "EtlFlowScheduler-Pool")
        dbCronJobs    <- updateCronJobsDB(transactor)
        _             <- cronJobs.update{_ => dbCronJobs.filter(_.schedule.isDefined)}
        _             <- Task(logger.info(s"Added/Updated jobs in database \n${dbCronJobs.mkString("\n")}"))
        _             <- Task(logger.info(s"Starting CRON JOB SCHEDULING for \n${dbCronJobs.filter(_.schedule.isDefined).mkString("\n")}"))
        scheduledFork <- scheduledTask(dbCronJobs,transactor).fork
      } yield new EtlFlow.Service {

        override def runJob(args: EtlJobArgs): ZIO[EtlFlowHas, Throwable, EtlJob] = {
          val job_deploy_mode = UF.getEtlJobName[EJN](args.name,etl_job_name_package).getActualProperties(Map.empty).job_deploy_mode
          if(job_deploy_mode.equalsIgnoreCase("remote")) {
            logger.info("Running job in remote mode ")
            runEtlJobRemote(args, transactor)
          } else {
            logger.info("Running job in local mode ")
            runEtlJobLocal(args, transactor)
          }
        //          val etlJobDetails: Task[(EJN, EtlFlowEtlJob, Map[String, String])] = Task {
          //            val job_name      = UF.getEtlJobName[EJN](args.name, etl_job_name_package)
          //            val props_map     = args.props.map(x => (x.key,x.value)).toMap
          //            val etl_job       = toEtlJob(job_name)(job_name.getActualProperties(props_map),globalProperties)
          //            etl_job.job_name  = job_name.toString
          //            (job_name,etl_job,props_map)
          //          }.mapError(e => ExecutionError(e.getMessage))
          //
          //          val job = for {
          //            queues                       <- subscribers.get
          //            (job_name,etl_job,props_map) <- etlJobDetails
          //            execution_props <- Task {
          //                                UF.convertToJsonByRemovingKeysAsMap(job_name.getActualProperties(props_map), List.empty)
          //                                  .map(x => (x._1, x._2.toString))
          //                              }.mapError{ e =>
          //                                logger.error(e.getMessage)
          //                                ExecutionError(e.getMessage)
          //                              }
          //            _               <- activeJobs.update(_ + 1)
          //            _               <- UIO.foreach(queues){queue =>
          //                                queue
          //                                  .offer(EtlJobStatus(etl_job.job_name,"Started",execution_props))
          //                                  .catchSomeCause {
          //                                    case cause if cause.interrupted =>
          //                                      subscribers.update(_.filterNot(_ == queue)).as(false)
          //                                  } // if queue was shutdown, remove from subscribers
          //                              }
          //            _               <- etl_job.execute().ensuring{
          //                                activeJobs.update(_ - 1) *>
          //                                  UIO.foreach(queues){queue =>
          //                                    queue
          //                                      .offer(EtlJobStatus(etl_job.job_name,"Completed",execution_props))
          //                                      .catchSomeCause {
          //                                        case cause if cause.interrupted =>
          //                                          subscribers.update(_.filterNot(_ == queue)).as(false)
          //                                      } // if queue was shutdown, remove from subscribers
          //                                  }
          //                              }.forkDaemon
          //          } yield EtlJob(args.name,execution_props)
          //          job
        }

        override def getEtlJobs: ZIO[EtlFlowHas, Throwable, List[EtlJob]] = {
          Task{
            UF.getEtlJobs[EJN].map(x => EtlJob(x,getJobDefaultProps(x))).toList
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

        override def addCronJob(args: CronJobArgs): ZIO[EtlFlowHas, Throwable, CronJob] = {
          val cronJobDB = CronJobDB(args.job_name, args.schedule.toString,0,0)
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

        override def getCronJobs: ZIO[EtlFlowHas, Throwable, List[CronJob]] = {
          val selectQuery = quote {
            querySchema[CronJobDB]("cronjob")
          }
          dc.run(selectQuery).transact(transactor)
            .map(y => y.map(x => CronJob(x.job_name, Cron(x.schedule).toOption, x.failed, x.success)))
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

        override def getJobs: ZIO[EtlFlowHas, Throwable, List[Job]] = {
          val selectQuery = quote {
            querySchema[CronJobDB]("cronjob")
          }
          dc.run(selectQuery)
            .transact(transactor)
            .map(y => y.map{x =>
              Job(x.job_name, getJobActualProps(x.job_name), Cron(x.schedule).toOption, x.failed, x.success)
            })
        }.mapError{ e =>
          logger.error(e.getMessage)
          ExecutionError(e.getMessage)
        }

        def getLogs: ZIO[EtlFlowHas, Throwable, EtlFlowMetrics] = {
          // ZStream.fromIterable(Seq(EtlFlowInfo(1), EtlFlowInfo(2), EtlFlowInfo(3))).runCollect.map(x => x.head)
          ZStream.fromIterable(Seq(EtlFlowMetrics(1,1,1,1), EtlFlowMetrics(2,2,2,2), EtlFlowMetrics(3,3,3,3))).runCollect.map(x => x.head)
          // Command("echo", "-n", "1\n2\n3").linesStream.runCollect.map(x => EtlFlowInfo(x.head.length))
        }
      }
    }

    def updateCronJobsDB(transactor: HikariTransactor[Task]): Task[List[CronJob]] = {

      val cronJobsDb = UF.getEtlJobs[EJN].map{x =>
        CronJobDB(
          x,
          UF.getEtlJobName[EJN](x,etl_job_name_package).getActualProperties(Map.empty).job_schedule,
          0,
          0
        )
      }

      val insertQuery = quote {
        liftQuery(cronJobsDb).foreach{e =>
          querySchema[CronJobDB]("cronjob")
            .insert(e)
            .onConflictUpdate(_.job_name)(
              _.schedule -> _.schedule,
            )
        }
      }
      dc.run(insertQuery).transact(transactor)

      val selectQuery = quote {
        querySchema[CronJobDB]("cronjob")
      }

      dc.run(insertQuery).transact(transactor) *> dc.run(selectQuery).transact(transactor)
        .map(y => y.map(x => CronJob(x.job_name, Cron(x.schedule).toOption, x.failed, x.success)))
    }

    def scheduledTask(dbCronJobs: List[CronJob], transactor: HikariTransactor[Task]): Task[Unit] = {
      val jobsToBeScheduled = dbCronJobs.flatMap{ cj =>
        if (cj.schedule.isDefined)
          List(cj)
        else
          List.empty
      }
      val cronSchedule = schedule(jobsToBeScheduled.map(cj => (cj.schedule.get,Stream.eval(
          UIO(logger.info(s"Starting CRON JOB ${cj.job_name} with schedule ${cj.schedule.toString} at ${UF.getCurrentTimestampAsString()}")) *> {
            val job_deploy_mode = UF.getEtlJobName[EJN](cj.job_name, etl_job_name_package).getActualProperties(Map.empty).job_deploy_mode
            if (job_deploy_mode.equalsIgnoreCase("remote")) {
              logger.info(s"Scheduled job ${cj.job_name} in remote mode ")
              runEtlJobRemote(EtlJobArgs(cj.job_name,List.empty), transactor)
            } else {
              logger.info(s"Scheduled job ${cj.job_name} in local mode ")
              runEtlJobLocal(EtlJobArgs(cj.job_name,List.empty), transactor)
            }
          }
      ))))
      cronSchedule.compile.drain
    }

    def getJobDefaultProps(jobName: String): Map[String, String] = {
      val name = UF.getEtlJobName[EJN](jobName,etl_job_name_package)
      val exclude_keys = List("job_run_id","job_description","job_properties")
      UF.convertToJsonByRemovingKeysAsMap(name.default_properties,exclude_keys).map(x =>(x._1,x._2.toString))
    }

    def getJobActualProps(jobName: String): Map[String, String] = {
      val name = UF.getEtlJobName[EJN](jobName, etl_job_name_package)
      val exclude_keys = List("job_run_id","job_description","job_properties")
      UF.convertToJsonByRemovingKeysAsMap(name.getActualProperties(Map.empty), exclude_keys).map(x => (x._1, x._2.toString))
    }
  }

  final def updateSuccessJob(job: String, transactor: HikariTransactor[Task]): Task[Long] = {
    val cronJobStringUpdate = quote {
      querySchema[CronJobDB]("cronjob")
        .filter(x => x.job_name == lift(job))
        .update{cj =>
          cj.success -> (cj.success + 1L)
        }
    }
    dc.run(cronJobStringUpdate).transact(transactor)
    }.mapError { e =>
    logger.error(e.getMessage)
    ExecutionError(e.getMessage)
  }
  final def updateFailedJob(job: String, transactor: HikariTransactor[Task]): Task[Long] = {
    val cronJobStringUpdate = quote {
      querySchema[CronJobDB]("cronjob")
        .filter(x => x.job_name == lift(job))
        .update{cj =>
          cj.failed -> (cj.failed + 1L)
        }
    }
    dc.run(cronJobStringUpdate).transact(transactor)
    }.mapError { e =>
      logger.error(e.getMessage)
      ExecutionError(e.getMessage)
  }

  def runEtlJobRemote(args: EtlJobArgs, transactor: HikariTransactor[Task]): Task[EtlJob]

  def runEtlJobLocal(args: EtlJobArgs, transactor: HikariTransactor[Task]): Task[EtlJob]

  val etlFlowLayer: Layer[Throwable, EtlFlowHas] = EtlFlowService.liveHttp4s
}
