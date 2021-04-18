package etlflow.executor

import java.io.{BufferedReader, InputStreamReader}
import etlflow.etljobs.{EtlJob, SequentialEtlJob}
import etlflow.utils.{JsonJackson, UtilityFunctions => UF}
import etlflow.{DBEnv, EtlJobProps, EtlJobPropsMapping, LocalExecutorEnv, LocalJobEnv}
import etlflow.utils.Executor.LOCAL_SUBPROCESS
import zio.{Layer, Task, UIO, ZEnv, ZIO}

object LocalExecutor {
  val live: Layer[Throwable, LocalExecutorEnv] = {
    UIO {
      new LocalExecutorService.Service {
        override def executeLocalSubProcessJob(name: String, properties: Map[String, String], config: LOCAL_SUBPROCESS): ZIO[LocalExecutorEnv, Throwable, Unit] = Task {
          logger.info(s"""Trying to submit job $name on local sub-process with Configurations:
                                  |job_name => $name
                                  |script_path => ${config.script_path}
                                  |min_heap_memory => ${config.heap_min_memory}
                                  |max_heap_memory => ${config.heap_max_memory}
                                  |properties => $properties""".stripMargin)



          val props = if(properties.isEmpty) "" else properties.map(x => s"${x._1}=${x._2}").mkString(",")

          val processBuilder = if (props != "")
            new ProcessBuilder("sh",s"${config.script_path}","run_job","--job_name",name,"--props",props)
          else
            new ProcessBuilder("sh",s"${config.script_path}","run_job","--job_name",name)

          val env = processBuilder.environment()
          env.put("JAVA_OPTS",s"${config.heap_min_memory} ${config.heap_max_memory}")

          val command = processBuilder.command().toString
          logger.info("Command = " + command)

          //Start the SubProcess
          val process = processBuilder.start()

          //Read the input logs from submitted sub process and show it in current process.
          val reader = new BufferedReader(new InputStreamReader(process.getInputStream));
          var readLine=reader.readLine()
          while(readLine != null){
            println(readLine)
            readLine = reader.readLine()
          }
          process.waitFor()

          logger.info("Exit Code: " + process.exitValue())

          //Get the exit code. If not equal to 0 then throw the exception otherwise return success.
          if(process.exitValue() != 0){
            logger.error(s"LOCAL SUB PROCESS JOB $name failed with error")
            throw new RuntimeException(s"LOCAL SUB PROCESS JOB $name failed with error")
          }
        }
        override def executeLocalJob(name: String, properties: Map[String, String], etl_job_name_package: String,job_run_id:Option[String] = None,is_master:Option[String] = None)
        : ZIO[LocalJobEnv, Throwable, Unit] = {
          val job_name = UF.getEtlJobName[EtlJobPropsMapping[EtlJobProps,EtlJob[EtlJobProps]]](name, etl_job_name_package)
          val job = job_name.etlJob(properties)
          job.job_name = job_name.toString
          job.job_enable_db_logging = job_name.job_enable_db_logging
          job.job_send_slack_notification = job_name.job_send_slack_notification
          job.job_notification_level = job_name.job_notification_level
          job.execute(job_run_id,is_master)
        }
        override def showLocalJobProps(name: String, properties: Map[String, String], etl_job_name_package: String): ZIO[LocalExecutorEnv, Throwable, Unit] = {
          val job_name = UF.getEtlJobName[EtlJobPropsMapping[EtlJobProps,EtlJob[EtlJobProps]]](name,etl_job_name_package)
          val exclude_keys = List("job_run_id","job_description","job_properties")
          val props = job_name.getActualProperties(properties)
          UIO(println(JsonJackson.convertToJsonByRemovingKeys(props,exclude_keys)))
        }
        override def showLocalJobStepProps(name: String, properties: Map[String, String], etl_job_name_package: String): ZIO[LocalExecutorEnv, Throwable, Unit] = {
          val job_name = UF.getEtlJobName[EtlJobPropsMapping[EtlJobProps,EtlJob[EtlJobProps]]](name,etl_job_name_package)
          val etl_job = job_name.etlJob(properties)
          if (etl_job.isInstanceOf[SequentialEtlJob[_]]) {
            etl_job.job_name = job_name.toString
            val json = JsonJackson.convertToJson(etl_job.getJobInfo(job_name.job_notification_level))
            UIO(println(json))
          }
          else {
            UIO(println("Step Props info not available for generic jobs"))
          }
        }
      }
    }.toLayer
  }
}
