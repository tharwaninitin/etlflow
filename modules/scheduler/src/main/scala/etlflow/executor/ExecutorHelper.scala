package etlflow.executor

import doobie.hikari.HikariTransactor
import etlflow.gcp.{DP, DPService}
import etlflow.local.{LOCAL, LocalService}
import etlflow.utils.EtlFlowHelper._
import etlflow.utils.db.Update
import etlflow.utils.Executor.{DATAPROC, KUBERNETES, LOCAL_SUBPROCESS}
import etlflow.utils.JDBC
import zio._
import zio.blocking.{Blocking, blocking}

trait ExecutorHelper extends K8SExecutor with EtlJobValidator {

  def runDataprocJob(args: EtlJobArgs, transactor: HikariTransactor[Task], config: DATAPROC, main_class: String, dp_libs: List[String], etl_job_name_package: String, sem: Semaphore): Task[EtlJob] = {
    for {
      etlJob  <- validateJob(args, etl_job_name_package)
      props_map = args.props.map(x => (x.key, x.value)).toMap
      _       <- sem.withPermit(blocking(DPService.executeSparkJob(args.name, props_map, main_class, dp_libs).provideLayer(DP.live(config)))
                .provideLayer(Blocking.live).foldM(
                ex => UIO(println(ex.getMessage)) *> Update.updateFailedJob(args.name, transactor),
                _ => Update.updateSuccessJob(args.name, transactor)
              )).forkDaemon
    } yield etlJob
  }

  def runLocalSubProcessJob(args: EtlJobArgs, transactor: HikariTransactor[Task], etl_job_name_package: String, config: LOCAL_SUBPROCESS, sem: Semaphore): Task[EtlJob] = {
    for {
      etlJob <- validateJob(args, etl_job_name_package)
      props_map = args.props.map(x => (x.key, x.value)).toMap
      _      <- sem.withPermit(blocking(LocalService.executeLocalJob(args.name, props_map).provideLayer(LOCAL.live(config)))
                .provideLayer(Blocking.live).foldM(
                ex => UIO(println(ex.getMessage)) *> Update.updateFailedJob(args.name, transactor),
                _ => Update.updateSuccessJob(args.name, transactor)
              )).forkDaemon
    } yield etlJob
  }

  def runKubernetesJob(args: EtlJobArgs, db: JDBC, transactor: HikariTransactor[Task], etl_job_name_package: String, config: KUBERNETES, sem: Semaphore): Task[EtlJob] = {
    for {
      etlJob <- validateJob(args, etl_job_name_package)
      _      <- sem.withPermit(blocking(runK8sJob(args,db,config)).provideLayer(Blocking.live).foldM(
                ex => UIO(println(ex.getMessage)) *> Update.updateFailedJob(args.name, transactor),
                _ => Update.updateSuccessJob(args.name, transactor)
              )).forkDaemon
    } yield etlJob
  }
}