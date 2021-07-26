package etlflow.coretests.steps.steps.parallel

import etlflow.coretests.steps.parallel.ParallelStepTestSuite.logger
import etlflow.etlsteps.{GenericETLStep, ParallelETLStep}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should

class ParallelPropsTestSuite extends AnyFlatSpec with should.Matchers {

  def processData(ip: Unit): Unit = {
    logger.info("Hello World")
  }

  val step1 = GenericETLStep(
    name               = "ProcessData",
    transform_function = processData,
  )

  val step2 = GenericETLStep(
    name               = "ProcessData",
    transform_function = processData,
  )
  val parstep = ParallelETLStep("ParallelStep")(step1,step2)

  parstep.job_run_id = "123"

  val props = parstep.getStepProperties()

  "getStepProperties should  " should "run successfully correct props" in {
    assert(props ==  Map("parallel_steps" -> "ProcessData,ProcessData", "step_run_id" -> "123"))
  }

}
