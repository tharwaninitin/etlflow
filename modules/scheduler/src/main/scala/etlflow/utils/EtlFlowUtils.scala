package etlflow.utils

import java.text.SimpleDateFormat
import java.time.LocalDateTime
import caliban.CalibanError.ExecutionError
import cron4s.Cron
import doobie.hikari.HikariTransactor
import etlflow.utils.EtlFlowHelper.{EtlJob, Job}
import etlflow.utils.db.Query
import etlflow.utils.{UtilityFunctions => UF}
import etlflow.{EtlJobName, EtlJobProps}
import org.slf4j.{Logger, LoggerFactory}
import zio.{Semaphore, Task, ZIO}
import cron4s.lib.javatime._
import scala.reflect.runtime.universe.TypeTag

trait EtlFlowUtils {
  lazy val logger: Logger = LoggerFactory.getLogger(getClass.getName)

  def getJobsFromDb[EJN <: EtlJobName[EJP] : TypeTag, EJP <: EtlJobProps : TypeTag](transactor: HikariTransactor[Task], etl_job_name_package: String): Task[List[Job]] = {
    for {
      rt    <- Task.runtime
      jobs  <- Query.getJobs(transactor)
        .map(y => y.map{x => {
          if(Cron(x.schedule).toOption.isDefined) {
            val sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm")
            val cron = Cron(x.schedule).toOption
            val endTime = sdf.parse(cron.get.next(LocalDateTime.now()).getOrElse("").toString).getTime
            val startTime = sdf.parse(LocalDateTime.now().toString).getTime
            val nextScheduleTime = cron.get.next(LocalDateTime.now()).getOrElse("").toString
            Job(x.job_name, getJobActualProps[EJN,EJP](x.job_name,etl_job_name_package), cron, nextScheduleTime, UF.getTimeDifferenceAsString(startTime,endTime), x.failed, x.success, x.is_active,UF.getEtlJobName[EJN](x.job_name,etl_job_name_package).getActualProperties(Map.empty).job_max_active_runs, UF.getEtlJobName[EJN](x.job_name,etl_job_name_package).getActualProperties(Map.empty).job_deploy_mode.toString)
          }else{
            Job(x.job_name, getJobActualProps[EJN,EJP](x.job_name,etl_job_name_package), None, "", "", x.failed, x.success, x.is_active,UF.getEtlJobName[EJN](x.job_name,etl_job_name_package).getActualProperties(Map.empty).job_max_active_runs, UF.getEtlJobName[EJN](x.job_name,etl_job_name_package).getActualProperties(Map.empty).job_deploy_mode.toString)
          }
        }})
    } yield jobs
  }.mapError{ e =>
    logger.error(e.getMessage)
    ExecutionError(e.getMessage)
  }
  def getEtlJobs[EJN <: EtlJobName[EJP] : TypeTag, EJP <: EtlJobProps : TypeTag](etl_job_name_package: String): Task[List[EtlJob]] = {
    Task{
      UF.getEtlJobs[EJN].map(x => EtlJob(x,getJobActualProps[EJN,EJP](x,etl_job_name_package))).toList
    }.mapError{ e =>
      logger.error(e.getMessage)
      ExecutionError(e.getMessage)
    }
  }
  def getJobActualProps[EJN <: EtlJobName[EJP] : TypeTag, EJP <: EtlJobProps : TypeTag](jobName: String, etl_job_name_package: String): Map[String, String] = {
    val name = UF.getEtlJobName[EJN](jobName, etl_job_name_package)
    val exclude_keys = List("job_run_id","job_description","job_properties")
    JsonJackson.convertToJsonByRemovingKeysAsMap(name.getActualProperties(Map.empty), exclude_keys).map(x => (x._1, x._2.toString))
  }
  def createSemaphores(jobs: List[EtlJob]): Task[Map[String, Semaphore]] = {
    for {
      rt          <- Task.runtime
      semaphores  = jobs.map(job => (job.name, rt.unsafeRun(Semaphore.make(permits = job.props("job_max_active_runs").toLong)))).toMap
    } yield semaphores
  }
}
