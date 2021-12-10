//package etlflow.log
//
//import etlflow.etlsteps.EtlStep
//import etlflow.utils.{ApplicationLogger, MapToJson}
//import etlflow.utils.DateTimeApi.{getCurrentTimestamp, getTimeDifferenceAsString}
//import zio.{RIO, Task, UIO, ULayer, ZIO, ZLayer}
//
//object Implementation extends ApplicationLogger {
//
//  private class StepLogger(etlStep: EtlStep[_, _], job_run_id: String) extends ApplicationLogger {
//
//    val remoteStep = List("EtlFlowJobStep", "DPSparkJobStep", "ParallelETLStep")
//
//    private def stringFormatter(value: String): String =
//      value.take(50).replaceAll("[^a-zA-Z0-9]", " ").replaceAll("\\s+", "_").toLowerCase
//
//    def update(start_time: Long, state_status: String, error_message: Option[String] = None, mode: String = "update"): ZIO[DBLogEnv, Throwable, Unit] = {
//      val step_name = stringFormatter(etlStep.name)
//      val properties = MapToJson(etlStep.getStepProperties)
//      val step_run_id = if (remoteStep.contains(etlStep.step_type)) etlStep.getStepProperties("step_run_id") else ""
//      if (mode == "insert") {
//        DBApi.insertStepRun(if (job_run_id == null) "" else job_run_id, step_name, properties, etlStep.step_type, step_run_id, start_time)
//      }
//      else {
//        val status = if (error_message.isDefined) state_status.toLowerCase() + " with error: " + error_message.get else state_status.toLowerCase()
//        val elapsed_time = getTimeDifferenceAsString(start_time, getCurrentTimestamp)
//        DBApi.updateStepRun(if (job_run_id == null) "" else job_run_id, step_name, properties, status, elapsed_time)
//      }
//    }
//  }
//
//  val live: ULayer[LogEnv] = ZLayer.succeed(
//
//    new LogWrapperApi.Service {
//
//      var job_run_id: String = null
//
//      override def setJobRunId(jri: String): UIO[Unit] = UIO {
//        job_run_id = jri
//      }
//
//      override def jobLogStart(start_time: Long, job_type: String, job_name: String, props: String, is_master: String): RIO[DBLogEnv with ConsoleLogEnv, Unit] = {
//        ConsoleApi.jobLogStart *>
//          DBApi.insertJobRun(job_run_id, job_name, props, job_type, is_master, start_time)
//      }
//
//      override def jobLogEnd(start_time: Long, job_run_id: String, job_name: String, ex: Option[Throwable]): RIO[DBLogEnv with ConsoleLogEnv with SlackLogEnv, Unit] = {
//        val elapsed_time = getTimeDifferenceAsString(start_time, getCurrentTimestamp)
//        val status = if (ex.isEmpty) "pass" else "failed with error: " + ex.get.getMessage
//        val console_status = if (ex.isEmpty) None else Some(status)
//        val slack_status = if (ex.isEmpty) None else Some(ex.get.getMessage)
//        ConsoleApi.jobLogEnd(console_status) *>
//          SlackApi.logJobEnd(job_name, job_run_id, start_time, slack_status) *>
//          DBApi.updateJobRun(job_run_id, status, elapsed_time) *>
//          (if (ex.isEmpty) ZIO.unit else Task.fail(new RuntimeException(ex.get.getMessage)))
//      }
//
//      override def stepLogStart(start_time: Long, etlStep: EtlStep[_, _]): RIO[DBLogEnv with ConsoleLogEnv, Unit] = {
//        val stepLogger = new StepLogger(etlStep, job_run_id)
//        ConsoleApi.stepLogStart(etlStep.name) *>
//          stepLogger.update(start_time, "started", mode = "insert")
//      }
//
//      override def stepLogEnd(start_time: Long, etlStep: EtlStep[_, _], ex: Option[Throwable]): RIO[DBLogEnv with ConsoleLogEnv with SlackLogEnv, Unit] = {
//        val stepLogger = new StepLogger(etlStep, job_run_id)
//        val status = if (ex.isEmpty) "pass" else "failed" + ex.get.getMessage
//        val error = if (ex.isEmpty) None else Some(ex.get.getMessage)
//        ConsoleApi.stepLogEnd(etlStep.name, if (ex.isEmpty) None else Some(ex.get.getStackTrace.mkString("\n"))) *>
//          SlackApi.logStepEnd(start_time, etlStep) *>
//          stepLogger.update(start_time, status, error) *>
//          (if (ex.isEmpty) ZIO.unit else Task.fail(new RuntimeException(ex.get.getMessage)))
//      }
//    }
//  )
//}
