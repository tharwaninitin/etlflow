package examples.jobs

import etlflow.etljobs.GenericEtlJob
import etlflow.etlsteps.{EtlFlowJobStep, GenericETLStep}
import examples.schema.MyEtlJobProps.LocalSampleProps
import zio.Schedule
import zio.duration._

case class EtlJob7DefinitionLocal(job_properties: LocalSampleProps) extends GenericEtlJob[LocalSampleProps] {

  val step1: EtlFlowJobStep[LocalSampleProps] = EtlFlowJobStep[LocalSampleProps](
    name     = "Child Job Submission for EtlJob2DefinitionLocal",
    job      = EtlJob2DefinitionLocal(LocalSampleProps())
  )

  override val job =  step1.execute()
}
