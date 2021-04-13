package etlflow.utils

import java.time.LocalDateTime
import caliban.CalibanError.ExecutionError
import cron4s.Cron
import cron4s.lib.javatime._
import doobie.hikari.HikariTransactor
import etlflow.etljobs.{EtlJob => CoreEtlJob}
import etlflow.log.ApplicationLogger
import etlflow.utils.EtlFlowHelper._
import etlflow.utils.JsonJackson._
import etlflow.utils.db.{Query, Update}
import etlflow.utils.{UtilityFunctions => UF}
import etlflow.{EtlJobProps, EtlJobPropsMapping}
import org.ocpsoft.prettytime.PrettyTime
import scalacache.memoization.memoizeSync
import scalacache.modes.sync._
import zio.{Semaphore, Task}
import scala.reflect.runtime.universe.TypeTag

trait EtlFlowUtils extends ApplicationLogger {

  implicit val jobPropsCache = CacheHelper.createCache[Map[String, String]]

  final def refreshJobsDB(transactor: HikariTransactor[Task], jobs: List[EtlJob]): Task[List[CronJob]] = {
    val jobsDB = jobs.map{x =>
      JobDB(
        x.name,
        job_description =  "",
        x.props.getOrElse("job_schedule",""),
        0,
        0,
        is_active = true
      )
    }
    if (jobsDB.isEmpty)
      Task{List.empty}
    else
      Update.refreshJobs(transactor, jobsDB)
  }

  final def getPropsCacheStats: CacheDetails = {
    val data:Map[String,String] = CacheHelper.toMap(jobPropsCache)
    val cacheInfo = CacheInfo("JobProps",
      jobPropsCache.underlying.stats.hitCount(),
      jobPropsCache.underlying.stats.hitRate(),
      jobPropsCache.underlying.asMap().size(),
      jobPropsCache.underlying.stats.missCount(),
      jobPropsCache.underlying.stats.missRate(),
      jobPropsCache.underlying.stats.requestCount(),
      data
    )
    CacheDetails("JobProps",JsonJackson.convertToJsonByRemovingKeysAsMap(cacheInfo,List("data")).mapValues(x => (x.toString)))
  }

  final def getJobsFromDb[EJN <: EtlJobPropsMapping[EtlJobProps,CoreEtlJob[EtlJobProps]] : TypeTag](transactor: HikariTransactor[Task], etl_job_name_package: String): Task[List[Job]] = {
    Query.getJobs(transactor)
      .map(y => y.map{x => {
        val props = getJobPropsMapping[EJN](x.job_name,etl_job_name_package)
        val p = new PrettyTime()
        val lastRunTime = x.last_run_time.map(ts => p.format(UF.getLocalDateTimeFromTimestamp(ts))).getOrElse("")

        if(Cron(x.schedule).toOption.isDefined) {
          val cron = Cron(x.schedule).toOption
          val startTimeMillis: Long =  UF.getCurrentTimestampUsingLocalDateTime
          val endTimeMillis: Option[Long] = cron.get.next(LocalDateTime.now()).map(dt => UF.getTimestampFromLocalDateTime(dt))
          val remTime1 = endTimeMillis.map(ts => UF.getTimeDifferenceAsString(startTimeMillis,ts)).getOrElse("")
          val remTime2 = endTimeMillis.map(ts => p.format(UF.getLocalDateTimeFromTimestamp(ts))).getOrElse("")

          val nextScheduleTime = cron.get.next(LocalDateTime.now()).getOrElse("").toString
          Job(x.job_name, props, cron, nextScheduleTime, s"$remTime2 ($remTime1)", x.failed, x.success, x.is_active,props("job_max_active_runs").toInt, props("job_deploy_mode"),x.last_run_time.getOrElse(0),s"$lastRunTime")
        }else{
          Job(x.job_name, props, None, "", "", x.failed, x.success, x.is_active,props("job_max_active_runs").toInt, props("job_deploy_mode"),x.last_run_time.getOrElse(0),s"$lastRunTime")
        }
      }})
  }.mapError{ e =>
    logger.error(e.getMessage)
    ExecutionError(e.getMessage)
  }

  final def getEtlJobs[EJN <: EtlJobPropsMapping[EtlJobProps,CoreEtlJob[EtlJobProps]] : TypeTag](etl_job_name_package: String): Task[List[EtlJob]] = {
    Task {
      UF.getEtlJobs[EJN].map(x => EtlJob(x, getJobPropsMapping[EJN](x,etl_job_name_package))).toList
    }.mapError{ e =>
      logger.error(e.getMessage)
      ExecutionError(e.getMessage)
    }
  }

  final def getJobPropsMapping[EJN <: EtlJobPropsMapping[EtlJobProps,CoreEtlJob[EtlJobProps]] : TypeTag](jobName: String, etl_job_name_package: String): Map[String, String] = memoizeSync[Map[String, String]](None){
    val props_mapping = UF.getEtlJobName[EJN](jobName,etl_job_name_package)
    convertToJsonByRemovingKeysAsMap(props_mapping.getProps,List.empty).map(x => (x._1, x._2.toString))
  }

  final def createSemaphores(jobs: List[EtlJob]): Task[Map[String, Semaphore]] = {
    for {
      rt          <- Task.runtime
      semaphores  = jobs.map(job => (job.name, rt.unsafeRun(Semaphore.make(permits = job.props("job_max_active_runs").toLong)))).toMap
    } yield semaphores
  }
}
