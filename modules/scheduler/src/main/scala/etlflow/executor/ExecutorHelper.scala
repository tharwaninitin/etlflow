package etlflow.executor

import caliban.CalibanError.ExecutionError
import doobie.hikari.HikariTransactor
import etlflow.gcp.{DP, DPService}
import etlflow.local.{LOCAL, LocalService}
import etlflow.scheduler.api.EtlFlowHelper._
import etlflow.scheduler.db.Update
import etlflow.utils.Executor.{DATAPROC, KUBERNETES, LOCAL_SUBPROCESS}
import etlflow.utils.{JDBC, JsonJackson, UtilityFunctions => UF}
import etlflow.{EtlJobName, EtlJobProps}
import zio._
import zio.blocking.{Blocking, blocking}

trait ExecutorHelper extends K8SExecutor {

  final def validateJob(args: EtlJobArgs, etl_job_name_package: String): Task[EtlJob] = {
    val etlJobDetails: Task[(EtlJobName[EtlJobProps], Map[String, String])] = Task {
      val job_name = UF.getEtlJobName[EtlJobName[EtlJobProps]](args.name, etl_job_name_package)
      val props_map = args.props.map(x => (x.key, x.value)).toMap
      (job_name, props_map)
    }.mapError { e =>
      println(e.getMessage)
      ExecutionError(e.getMessage)
    }
    for {
      (job_name, props_map) <- etlJobDetails
      execution_props <- Task {
        JsonJackson.convertToJsonByRemovingKeysAsMap(job_name.getActualProperties(props_map), List.empty)
          .map(x => (x._1, x._2.toString))
      }.mapError { e =>
        println(e.getMessage)
        ExecutionError(e.getMessage)
      }
    } yield EtlJob(args.name, execution_props)
  }

  def runDataprocJob(args: EtlJobArgs, transactor: HikariTransactor[Task], config: DATAPROC, main_class: String, dp_libs: List[String], etl_job_name_package: String): Task[EtlJob] = {
    for {
      etlJob  <- validateJob(args, etl_job_name_package)
      props_map = args.props.map(x => (x.key, x.value)).toMap
      _       <- blocking(DPService.executeSparkJob(args.name, props_map, main_class, dp_libs).provideLayer(DP.live(config)))
                .provideLayer(Blocking.live).foldM(
                ex => UIO(println(ex.getMessage)) *> Update.updateFailedJob(args.name, transactor),
                _ => Update.updateSuccessJob(args.name, transactor)
              ).forkDaemon
    } yield etlJob
  }

  def runLocalSubProcessJob(args: EtlJobArgs, transactor: HikariTransactor[Task], etl_job_name_package: String, config: LOCAL_SUBPROCESS): Task[EtlJob] = {
    for {
      etlJob <- validateJob(args, etl_job_name_package)
      props_map = args.props.map(x => (x.key, x.value)).toMap
      _      <- blocking(LocalService.executeLocalJob(args.name, props_map).provideLayer(LOCAL.live(config)))
                .provideLayer(Blocking.live).foldM(
                ex => UIO(println(ex.getMessage)) *> Update.updateFailedJob(args.name, transactor),
                _ => Update.updateSuccessJob(args.name, transactor)
              ).forkDaemon
    } yield etlJob
  }

  def runKubernetesJob(args: EtlJobArgs, db: JDBC, transactor: HikariTransactor[Task], etl_job_name_package: String, config: KUBERNETES): Task[EtlJob] = {
    for {
      etlJob <- validateJob(args, etl_job_name_package)
      _      <- blocking(runK8sJob(args,db,config)).provideLayer(Blocking.live).foldM(
                ex => UIO(println(ex.getMessage)) *> Update.updateFailedJob(args.name, transactor),
                _ => Update.updateSuccessJob(args.name, transactor)
              ).forkDaemon
    } yield etlJob
  }
}