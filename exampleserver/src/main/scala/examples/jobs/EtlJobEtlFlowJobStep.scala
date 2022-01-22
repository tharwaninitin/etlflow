package examples.jobs

import etlflow.etljobs.EtlJob
import etlflow.etlsteps.EtlFlowJobStep
import examples.schema.MyEtlJobProps.LocalSampleProps

case class EtlJobEtlFlowJobStep(job_properties: LocalSampleProps) extends EtlJob[LocalSampleProps] {

  val step1: EtlFlowJobStep[LocalSampleProps] = EtlFlowJobStep[LocalSampleProps](
    name = "master_job_submission_etlJob2DefinitionLocal",
    job = EtlJobGenericStep(LocalSampleProps())
  )

  override val job = step1.execute
}
