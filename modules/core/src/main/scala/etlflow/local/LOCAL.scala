package etlflow.local

import java.io.{BufferedReader, InputStreamReader}

import etlflow.utils.Executor.LOCAL_SUBPROCESS
import zio.{Layer, Task, ZIO, ZLayer}

object LOCAL {

  def live(config: LOCAL_SUBPROCESS) : Layer[Throwable, LocalService] = ZLayer.fromEffect {
    Task {
      new LocalService.Service {
        override def executeLocalJob(name: String, properties: Map[String, String]): ZIO[LocalService, Throwable, Unit] = Task {
          gcp_logger.info(s"""Trying to submit job $name on local sub-process with Configurations:
                             |job_name => $name
                             |script_path => ${config.script_path}
                             |min_heap_memory => ${config.heap_min_memory}
                             |max_heap_memory => ${config.heap_max_memory}
                             |properties => $properties""".stripMargin)

          val props = properties.map(x => s"${x._1}=${x._2}").mkString(",")

          val processBuilder = if (props != "=")
             new ProcessBuilder("sh",s"${config.script_path}","run_job","--job_name",name,"--props",props)
          else
             new ProcessBuilder("sh",s"${config.script_path}","run_job","--job_name",name)

          val env = processBuilder.environment()
          env.put("JAVA_OPTS",s"${config.heap_min_memory} ${config.heap_max_memory}")

          val command = processBuilder.command().toString()
          gcp_logger.info("Command = " + command)

          //Start the subprocess
          val process = processBuilder.start()

          //Read the input logs from submitted sub process and show it in current process.
          val reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
          var readLine=reader.readLine()
          while(readLine != null){
            println(readLine)
            readLine = reader.readLine()
          }
          process.waitFor()

          gcp_logger.info("Exit Code :" + process.exitValue())

          //Get the exit code. If 1 then throw the execption otherwise return success.
          if( process.exitValue() == 1 ){
            throw new RuntimeException(s"Job $name failed with error")
          }
        }
      }
    }
  }
}
