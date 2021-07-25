package etlflow.coretests.steps.master

import etlflow.coretests.Schema.EtlJob1Props
import etlflow.coretests.jobs.Job1HelloWorld
import etlflow.etlsteps.EtlFlowJobStep
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should

class EtlFlowJobPropsTestSuite extends AnyFlatSpec with should.Matchers {

  val step = EtlFlowJobStep[EtlJob1Props](
    name = "Test",
    job  = Job1HelloWorld(EtlJob1Props()),
  )

  step.job_run_id = "123"
  val props = step.getStepProperties()

  "getStepProperties should  " should "run successfully correct props" in {
    assert(props ==  Map("step_run_id" -> "123"))
  }

}
