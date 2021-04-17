package etlflow.webserver.api

import etlflow.executor.Executor
import etlflow.jdbc.{DB, DBEnv}
import etlflow.log.{JobRun, StepRun}
import etlflow.utils.EtlFlowHelper._
import etlflow.utils.{CacheHelper, Config, EtlFlowUtils, JsonJackson, QueueHelper, UtilityFunctions => UF}
import etlflow.{EJPMType, BuildInfo => BI}
import scalacache.caffeine.CaffeineCache
import zio._
import zio.blocking.Blocking
import zio.clock.Clock
import zio.stream.ZStream
import scala.reflect.runtime.universe.TypeTag

object ApiImplementation extends EtlFlowUtils with Executor {

  def live[EJN <: EJPMType : TypeTag](
    cache: CaffeineCache[String]
    ,cronJobs: Ref[List[CronJob]]
    ,jobSemaphores: Map[String, Semaphore]
    ,jobs: List[EtlJob]
    ,jobQueue: Queue[(String,String,String,String)]
    ,config: Config
  ): ZLayer[Blocking, Throwable, GQLEnv] = {
    for {
      subscribers           <- Ref.make(List.empty[Queue[EtlJobStatus]])
      activeJobs            <- Ref.make(0)
      etl_job_name_package  = UF.getJobNamePackage[EJN] + "$"
      mb                    = 1024*1024
      javaRuntime           = java.lang.Runtime.getRuntime
    } yield new ApiService {

      def getLoginCacheStats:CacheDetails = {
        val data:Map[String,String] = CacheHelper.toMap(cache)
        val cacheInfo = CacheInfo("Login",
          cache.underlying.stats.hitCount(),
          cache.underlying.stats.hitRate(),
          cache.underlying.asMap().size(),
          cache.underlying.stats.missCount(),
          cache.underlying.stats.missRate(),
          cache.underlying.stats.requestCount(),
          data
        )
        CacheDetails("Login",JsonJackson.convertToJsonByRemovingKeysAsMap(cacheInfo,List("data")).mapValues(x => (x.toString)))
      }

      override def getJobs: ZIO[GQLEnv with DBEnv, Throwable, List[Job]] = DB.getJobs[EJN](etl_job_name_package)

      override def getCacheStats: ZIO[GQLEnv, Throwable, List[CacheDetails]] = Task(List(getPropsCacheStats,getLoginCacheStats))

      override def getQueueStats: ZIO[GQLEnv, Throwable, List[QueueDetails]] = QueueHelper.takeAll(jobQueue)

      override def getJobLogs(args: JobLogsArgs): ZIO[GQLEnv with DBEnv, Throwable, List[JobLogs]] = DB.getJobLogs(args)

      override def getCredentials: ZIO[GQLEnv with DBEnv, Throwable, List[GetCredential]] = DB.getCredentials

      override def runJob(args: EtlJobArgs, submitter: String): ZIO[GQLEnv with Blocking with Clock with DBEnv, Throwable, EtlJob] = {
        runActiveEtlJob[EJN](args,jobSemaphores(args.name),config,etl_job_name_package,submitter,jobQueue)
      }

      override def getDbStepRuns(args: DbStepRunArgs): ZIO[GQLEnv with DBEnv, Throwable, List[StepRun]] = DB.getStepRuns(args)

      override def getDbJobRuns(args: DbJobRunArgs): ZIO[GQLEnv with DBEnv, Throwable, List[JobRun]] = DB.getJobRuns(args)

      override def updateJobState(args: EtlJobStateArgs): ZIO[GQLEnv with DBEnv, Throwable, Boolean] = DB.updateJobState(args)

      override def login(args: UserArgs): ZIO[GQLEnv with DBEnv, Throwable, UserAuth] =  Authentication.login(args,cache,config)

      override def getInfo: ZIO[GQLEnv, Throwable, EtlFlowMetrics] = {
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

      override def getCurrentTime: ZIO[GQLEnv, Throwable, CurrentTime] = UIO(CurrentTime(current_time = UF.getCurrentTimestampAsString()))

      override def addCredentials(args: CredentialsArgs): ZIO[GQLEnv with DBEnv, Throwable, Credentials] = DB.addCredential(args)

      override def updateCredentials(args: CredentialsArgs): ZIO[GQLEnv with DBEnv, Throwable, Credentials] = DB.updateCredential(args)

      override def notifications: ZStream[GQLEnv, Nothing, EtlJobStatus] = ZStream.unwrap {
        for {
          queue <- Queue.unbounded[EtlJobStatus]
          _     <- UIO(logger.info(s"Starting new subscriber"))
          _     <- subscribers.update(queue :: _)
        } yield ZStream.fromQueue(queue).ensuring(queue.shutdown)
      }
    }
  }.toLayer
}
