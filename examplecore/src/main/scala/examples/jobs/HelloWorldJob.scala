package examples.jobs

import etlflow.JobEnv
import etlflow.etljobs.GenericEtlJob
import etlflow.etlsteps.GenericETLStep
import examples.schema.MyEtlJobProps.EtlJob1Props
import zio.ZIO

case class HelloWorldJob(job_properties: EtlJob1Props) extends GenericEtlJob[EtlJob1Props] {

  def processData(ip: Unit): Unit = {
    logger.info("Hello World")
    throw  new RuntimeException("Error123")
  }

  val step1 = GenericETLStep(
                name               = "Step_1",
                transform_function = processData,
              )

  override def job: ZIO[JobEnv, Throwable, Unit] = step1.execute(())
}
