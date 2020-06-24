package etlflow.scheduler


import java.util.concurrent.TimeUnit

import scala.collection.JavaConverters._
import caliban.CalibanError.ExecutionError
import com.google.cloud.dataproc.v1.{Job, JobControllerClient, JobControllerSettings, JobPlacement, SparkJob}
import doobie.hikari.HikariTransactor
import etlflow.scheduler.EtlFlowHelper._
import etlflow.utils.{DataprocHelper, GlobalProperties, UtilityFunctions => UF}
import etlflow.{EtlJobName, EtlJobProps}
import zio._

import scala.reflect.runtime.universe.TypeTag
import etlflow.etljobs.{EtlJob => EtlFlowEtlJob}

abstract class Schedulers[EJN <: EtlJobName[EJP] : TypeTag, EJP <: EtlJobProps : TypeTag, EJGP <: GlobalProperties : TypeTag]
  extends SchedulerApp[EJN,EJP,EJGP] with DataprocHelper  {

  val main_class: String
  val dp_libs: List[String]
  val gcp_region: String
  val gcp_project: String
  val gcp_dp_endpoint: String
  val gcp_dp_cluster_name: String

  def toEtlJob(job_name: EJN): (EJP,Option[EJGP]) => EtlFlowEtlJob

  final override def runEtlJob(args: EtlJobArgs, transactor: HikariTransactor[Task]): Task[EtlJob] = {
    val etlJobDetails: Task[(EJN, Map[String, String])] = Task {
      val job_name      = UF.getEtlJobName[EJN](args.name, etl_job_name_package)
      val props_map     = args.props.map(x => (x.key,x.value)).toMap
      (job_name,props_map)
    }.mapError{ e =>
      logger.error(e.getMessage)
      ExecutionError(e.getMessage)
    }

    for {
      (job_name,props_map) <- etlJobDetails
      execution_props  <- Task {
        UF.convertToJsonByRemovingKeysAsMap(job_name.getActualProperties(props_map), List.empty)
          .map(x => (x._1, x._2.toString))
      }.mapError{ e =>
        logger.error(e.getMessage)
        ExecutionError(e.getMessage)
      }
      _  <- executeDataProcJob(job_name.toString,props_map).foldM(
        ex => updateFailedJob(job_name.toString,transactor),
        _  => updateSuccessJob(job_name.toString,transactor)
      ).forkDaemon
    } yield EtlJob(args.name,execution_props)
  }

  final override def runEtlJobLocal(args: EtlJobArgs, transactor: HikariTransactor[Task]): Task[EtlJob] = {

    val etlJobDetails: Task[(EJN, EtlFlowEtlJob, Map[String, String])] = Task {
      val job_name      = UF.getEtlJobName[EJN](args.name, etl_job_name_package)
      val props_map     = args.props.map(x => (x.key,x.value)).toMap
      val etl_job       = toEtlJob(job_name)(job_name.getActualProperties(props_map),globalProperties)
      etl_job.job_name  = job_name.toString
      (job_name,etl_job,props_map)
    }.mapError{ e =>
      logger.error(e.getMessage)
      ExecutionError(e.getMessage)
    }

    for {
      (job_name,etl_job,props_map) <- etlJobDetails
      execution_props   <- Task {
        UF.convertToJsonByRemovingKeysAsMap(job_name.getActualProperties(props_map), List.empty)
          .map(x => (x._1, x._2.toString))
      }.mapError{ e =>
        logger.error(e.getMessage)
        ExecutionError(e.getMessage)
      }
      _                  <- etl_job.execute().foldM(
        ex => updateFailedJob(job_name.toString,transactor),
        _  => updateSuccessJob(job_name.toString,transactor)
      ).forkDaemon
    } yield EtlJob(args.name,execution_props)
  }
}
