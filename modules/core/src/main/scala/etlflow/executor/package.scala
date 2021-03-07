package etlflow

import etlflow.utils.Executor.LOCAL_SUBPROCESS
import org.slf4j.{Logger, LoggerFactory}
import zio.{Has, ZIO}

package object executor {
  val executor_logger: Logger = LoggerFactory.getLogger(getClass.getName)

  type LocalExecutorService = Has[LocalExecutorService.Service]

  object LocalExecutorService {
    trait Service {
      def executeLocalSubProcessJob(name: String, properties: Map[String,String], config: LOCAL_SUBPROCESS): ZIO[LocalExecutorService, Throwable, Unit]
      def executeLocalJob(name: String, properties: Map[String,String], etl_job_name_package: String,job_run_id:Option[String] = None,is_master:Option[String] = None): ZIO[LocalExecutorService, Throwable, Unit]
      def showLocalJobProps(name: String, properties: Map[String,String], etl_job_name_package: String): ZIO[LocalExecutorService, Throwable, Unit]
      def showLocalJobStepProps(name: String, properties: Map[String,String], etl_job_name_package: String): ZIO[LocalExecutorService, Throwable, Unit]
    }
    def executeLocalSubProcessJob(name: String, properties: Map[String,String], config: LOCAL_SUBPROCESS): ZIO[LocalExecutorService, Throwable, Unit] =
      ZIO.accessM(_.get.executeLocalSubProcessJob(name, properties, config))
    def executeLocalJob(name: String, properties: Map[String,String], etl_job_name_package: String,job_run_id:Option[String] = None,is_master:Option[String] = None): ZIO[LocalExecutorService, Throwable, Unit] =
      ZIO.accessM(_.get.executeLocalJob(name, properties, etl_job_name_package,job_run_id,is_master))
    def showLocalJobProps(name: String, properties: Map[String,String], etl_job_name_package: String): ZIO[LocalExecutorService, Throwable, Unit] =
      ZIO.accessM(_.get.showLocalJobProps(name, properties, etl_job_name_package))
    def showLocalJobStepProps(name: String, properties: Map[String,String], etl_job_name_package: String): ZIO[LocalExecutorService, Throwable, Unit] =
      ZIO.accessM(_.get.showLocalJobStepProps(name, properties, etl_job_name_package))
  }
}
