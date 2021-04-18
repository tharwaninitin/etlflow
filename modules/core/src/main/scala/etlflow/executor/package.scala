package etlflow

import etlflow.log.ApplicationLogger
import etlflow.utils.Executor.LOCAL_SUBPROCESS
import zio.{Has, ZEnv, ZIO}

package object executor extends ApplicationLogger {

  type LocalExecutorService = Has[LocalExecutorService.Service]

  object LocalExecutorService {
    trait Service {
      def executeLocalSubProcessJob(name: String, properties: Map[String,String], config: LOCAL_SUBPROCESS): ZIO[LocalExecutorService, Throwable, Unit]
      def executeLocalJob(name: String, properties: Map[String,String], etl_job_name_package: String,job_run_id:Option[String] = None,is_master:Option[String] = None)
      : ZIO[ZEnv with LocalExecutorService with TransactorEnv, Throwable, Unit]
      def showLocalJobProps(name: String, properties: Map[String,String], etl_job_name_package: String): ZIO[LocalExecutorService, Throwable, Unit]
      def showLocalJobStepProps(name: String, properties: Map[String,String], etl_job_name_package: String): ZIO[LocalExecutorService, Throwable, Unit]
    }
    def executeLocalSubProcessJob(name: String, properties: Map[String,String], config: LOCAL_SUBPROCESS): ZIO[LocalExecutorService, Throwable, Unit] =
      ZIO.accessM(_.get.executeLocalSubProcessJob(name, properties, config))
    def executeLocalJob(name: String, properties: Map[String,String], etl_job_name_package: String,job_run_id:Option[String] = None,is_master:Option[String] = None)
    : ZIO[ZEnv with LocalExecutorService with TransactorEnv, Throwable, Unit] =
      ZIO.accessM[LocalExecutorService with TransactorEnv with ZEnv](_.get.executeLocalJob(name, properties, etl_job_name_package,job_run_id,is_master))
    def showLocalJobProps(name: String, properties: Map[String,String], etl_job_name_package: String): ZIO[LocalExecutorService, Throwable, Unit] =
      ZIO.accessM(_.get.showLocalJobProps(name, properties, etl_job_name_package))
    def showLocalJobStepProps(name: String, properties: Map[String,String], etl_job_name_package: String): ZIO[LocalExecutorService, Throwable, Unit] =
      ZIO.accessM(_.get.showLocalJobStepProps(name, properties, etl_job_name_package))
  }
}
