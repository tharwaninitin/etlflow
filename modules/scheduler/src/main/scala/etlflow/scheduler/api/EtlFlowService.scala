package etlflow.scheduler.api

import caliban.CalibanError.ExecutionError
import cron4s.Cron
import doobie.hikari.HikariTransactor
import doobie.quill.DoobieContext
import etlflow.log.{JobRun, StepRun}
import etlflow.scheduler.api.EtlFlowHelper._
import etlflow.scheduler.db.Query
import etlflow.utils.Executor.{DATAPROC, LOCAL}
import etlflow.utils.{JsonJackson, UtilityFunctions => UF}
import etlflow.{EtlJobName, EtlJobProps}
import io.getquill.Literal
import org.slf4j.{Logger, LoggerFactory}
import scalacache.Cache
import zio._
import zio.blocking.Blocking
import zio.stream.ZStream

import scala.reflect.runtime.universe.TypeTag

trait EtlFlowService {
  lazy val logger: Logger = LoggerFactory.getLogger(getClass.getName)

  val dc = new DoobieContext.Postgres(Literal)
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
        Query.updateJobState(args,transactor)
      }

      override def login(args: UserArgs): ZIO[EtlFlowHas, Throwable, UserAuth] =  {
        Query.login(args,transactor,cache)
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
        Query.addCredentials(args,transactor)
      }

      override def updateCredentials(args: CredentialsArgs): ZIO[EtlFlowHas, Throwable, Credentials] = {
        Query.updateCredentials(args,transactor)
      }

      override def addCronJob(args: CronJobArgs): ZIO[EtlFlowHas, Throwable, CronJob] = {
        Query.addCronJob(args,transactor)
      }

      override def updateCronJob(args: CronJobArgs): ZIO[EtlFlowHas, Throwable, CronJob] = {
       Query.updateCronJob(args,transactor)
      }

      override def getDbStepRuns(args: DbStepRunArgs): ZIO[EtlFlowHas, Throwable, List[StepRun]] = {
        Query.getDbStepRuns(args,transactor)
      }

      override def getDbJobRuns(args: DbJobRunArgs): ZIO[EtlFlowHas, Throwable, List[JobRun]] = {
        Query.getDbJobRuns(args,transactor)
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
        Query.getJobs(transactor)
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
