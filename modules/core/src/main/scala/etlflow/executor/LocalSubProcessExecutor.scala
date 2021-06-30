package etlflow.executor

import etlflow.schema.Executor.LOCAL_SUBPROCESS
import etlflow.utils.ApplicationLogger
import zio.ZIO
import zio.blocking.{Blocking, effectBlocking}

import java.io.{BufferedReader, InputStreamReader}

case class LocalSubProcessExecutor(config: LOCAL_SUBPROCESS) extends ApplicationLogger with Service {
  override def executeJob(name: String, properties: Map[String, String]): ZIO[Blocking, Throwable, Unit] = effectBlocking {
    logger.info(s"""Trying to submit job $name on local sub-process with Configurations:
                   |job_name => $name
                   |script_path => ${config.script_path}
                   |min_heap_memory => ${config.heap_min_memory}
                   |max_heap_memory => ${config.heap_max_memory}
                   |properties => $properties""".stripMargin
    )

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
}