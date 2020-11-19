package etlflow.executor

import java.io.{BufferedReader, InputStreamReader}
import etlflow.utils.{UtilityFunctions => UF}
import etlflow.{EtlJobName, EtlJobProps}
import etlflow.utils.Executor.LOCAL_SUBPROCESS
import zio.{Layer, Task, ZIO, ZLayer, ZEnv}

object LocalExecutor {
  val live: Layer[Throwable, LocalExecutorService] = ZLayer.fromEffect {
    Task {
      new LocalExecutorService.Service {
        override def executeLocalSubProcessJob(name: String, properties: Map[String, String], config: LOCAL_SUBPROCESS): ZIO[LocalExecutorService, Throwable, Unit] = Task {
          executor_logger.info(s"""Trying to submit job $name on local sub-process with Configurations:
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
          executor_logger.info("Command = " + command)

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

          executor_logger.info("Exit Code: " + process.exitValue())

          //Get the exit code. If not equal to 0 then throw the exception otherwise return success.
          if(process.exitValue() != 0){
            executor_logger.error(s"LOCAL SUB PROCESS JOB $name failed with error")
            throw new RuntimeException(s"LOCAL SUB PROCESS JOB $name failed with error")
          }
        }
        override def executeLocalJob(name: String, properties: Map[String, String], etl_job_name_package: String,job_run_id:Option[String] = None,is_master:Option[String] = None): ZIO[LocalExecutorService, Throwable, Unit] = {
          val job_name = UF.getEtlJobName[EtlJobName[EtlJobProps]](name, etl_job_name_package)
          val job = job_name.etlJob(properties)
          job.job_name = job_name.toString
          job.execute(job_run_id,is_master).provideLayer(ZEnv.live)
        }
      }
    }
  }
}
