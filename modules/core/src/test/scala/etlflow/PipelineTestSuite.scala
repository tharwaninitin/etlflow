package etlflow

import etlflow.audit.Audit
import etlflow.log.ApplicationLogger
import etlflow.task.{EtlTask, GenericTask}
import zio.ZIO
import zio.test.Assertion.equalTo
import zio.test._

@SuppressWarnings(Array("org.wartremover.warts.Throw"))
object PipelineTestSuite extends ApplicationLogger {
  def processData(msg: String): Unit = logger.info(s"Hello World from $msg")

  val task1: GenericTask[Unit] = GenericTask(
    name = "ProcessData1",
    function = processData("ProcessData1")
  )

  val task2: GenericTask[Unit] = GenericTask(
    name = "ProcessData2",
    function = processData("ProcessData2")
  )

  val task3: GenericTask[Unit] = GenericTask(
    name = "ProcessData3",
    function = processData("ProcessData3")
  )

  val task4: GenericTask[Unit] = GenericTask(
    name = "ProcessData4",
    function = processData("ProcessData4")
  )

  val spec: Spec[Audit, Any] =
    suite("Generic Task")(
      test("Test GenericETLTask pipeline") {
        logger.info(s"EtlTask.RT ${EtlTask.RT.get()}")
        val pipeline: EtlTask[Any, Unit] = task1 *> task2 *> task3 *> task4
        logger.info(s"EtlTask.RT ${EtlTask.RT.get()}")
        logger.info(s"Task Name ${pipeline.name}")
        val pipelineZIO = pipeline.execute.provide(audit.console)
        assertZIO(pipelineZIO.foldZIO(ex => ZIO.succeed(ex.getMessage), _ => ZIO.succeed("ok")))(equalTo("ok"))
      },
      test("Test GenericETLTask for pipeline") {
        logger.info(s"EtlTask.RT ${EtlTask.RT.get()}")
        val pipeline = for {
          _ <- task1
          _ <- task2
          _ <- task3
          _ <- task4
        } yield ()
        logger.info(s"EtlTask.RT ${EtlTask.RT.get()}")
        logger.info(s"Task Name ${pipeline.name}")
        val pipelineZIO = pipeline.execute.provide(audit.console)
        assertZIO(pipelineZIO.foldZIO(ex => ZIO.succeed(ex.getMessage), _ => ZIO.succeed("ok")))(equalTo("ok"))
      }
    )
}
