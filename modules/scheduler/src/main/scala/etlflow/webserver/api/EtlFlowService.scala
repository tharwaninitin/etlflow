package etlflow.webserver.api

import doobie.hikari.HikariTransactor
import etlflow.executor.Executor
import etlflow.log.{JobRun, StepRun}
import etlflow.utils.EtlFlowHelper._
import etlflow.utils.db.Query
import etlflow.utils.{CacheHelper, Config, EtlFlowUtils, QueueHelper, UtilityFunctions => UF}
import etlflow.{EtlJobName, EtlJobProps, BuildInfo => BI}
import scalacache.caffeine.CaffeineCache
import zio._
import zio.blocking.Blocking
import zio.stream.ZStream

import scala.reflect.runtime.universe.TypeTag

trait EtlFlowService extends EtlFlowUtils with Executor {

  val config: Config

  def liveHttp4s[EJN <: EtlJobName[EJP] : TypeTag, EJP <: EtlJobProps : TypeTag](
      transactor: HikariTransactor[Task],
      cache: CaffeineCache[String],
      cronJobs: Ref[List[CronJob]],
      jobSemaphores: Map[String, Semaphore],
      jobs: List[EtlJob],
      jobQueue: Queue[(String,String)]
    ): ZLayer[Blocking, Throwable, EtlFlowHas] = ZLayer.fromEffect{
    for {
      subscribers           <- Ref.make(List.empty[Queue[EtlJobStatus]])
      activeJobs            <- Ref.make(0)
      etl_job_name_package  = UF.getJobNamePackage[EJN] + "$"
      mb                    = 1024*1024
      javaRuntime           = java.lang.Runtime.getRuntime
    } yield new EtlFlow.Service {

      def getLoginCacheStats:CacheInfo = {
        val data:Map[String,String] = CacheHelper.toMap(cache)
        CacheInfo("Login",
          cache.underlying.stats.hitCount(),
          cache.underlying.stats.hitRate(),
          cache.underlying.asMap().size(),
          cache.underlying.stats.missCount(),
          cache.underlying.stats.missRate(),
          cache.underlying.stats.requestCount(),
          data
        )
      }

      override def getJobs: ZIO[EtlFlowHas, Throwable, List[Job]] = {
        getJobsFromDb[EJN,EJP](transactor,etl_job_name_package)
      }

      override def getCacheStats: ZIO[EtlFlowHas, Throwable, List[CacheInfo]] = {
        Task(List(Query.getJobCacheStats,getPropsCacheStats,getLoginCacheStats))
      }

      override def getQueueStats: ZIO[EtlFlowHas, Throwable, List[QueueInfo]] = {
        QueueHelper.takeAll(jobQueue)
      }

      override def runJob(args: EtlJobArgs): ZIO[EtlFlowHas, Throwable, Option[EtlJob]] = {
        runActiveEtlJob[EJN,EJP](args,transactor,jobSemaphores(args.name),config,etl_job_name_package,"Api",jobQueue)
      }

      override def getDbStepRuns(args: DbStepRunArgs): ZIO[EtlFlowHas, Throwable, List[StepRun]] = {
        Query.getDbStepRuns(args,transactor)
      }

      override def getDbJobRuns(args: DbJobRunArgs): ZIO[EtlFlowHas, Throwable, List[JobRun]] = {
        Query.getDbJobRuns(args,transactor)
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
        } yield EtlFlowMetrics(
          x,
          y.length,
          jobs.length,
          z.length,
          used_memory = ((javaRuntime.totalMemory - javaRuntime.freeMemory) / mb).toString,
          free_memory = (javaRuntime.freeMemory / mb).toString,
          total_memory = (javaRuntime.totalMemory / mb).toString,
          max_memory = (javaRuntime.maxMemory / mb).toString,
          current_time = UF.getCurrentTimestampAsString(),
          build_time = BI.builtAtString
        )
      }

      override def getCurrentTime: ZIO[EtlFlowHas, Throwable, CurrentTime] = {
        UIO(CurrentTime(current_time = UF.getCurrentTimestampAsString()))
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

      override def notifications: ZStream[EtlFlowHas, Nothing, EtlJobStatus] = ZStream.unwrap {
        for {
          queue <- Queue.unbounded[EtlJobStatus]
          _     <- UIO(logger.info(s"Starting new subscriber"))
          _     <- subscribers.update(queue :: _)
        } yield ZStream.fromQueue(queue).ensuring(queue.shutdown)
      }
    }
  }
}
