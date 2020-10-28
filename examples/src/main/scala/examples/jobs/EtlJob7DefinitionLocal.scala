package examples.jobs

import etlflow.etljobs.GenericEtlJob
import etlflow.etlsteps.EtlFlowJobStep
import examples.schema.MyEtlJobProps.LocalSampleProps

case class EtlJob7DefinitionLocal(job_properties: LocalSampleProps) extends GenericEtlJob[LocalSampleProps] {

  val step1: EtlFlowJobStep[LocalSampleProps] = EtlFlowJobStep[LocalSampleProps](
    name     = "master_job_submission_etlJob2DefinitionLocal",
    job      = EtlJob2DefinitionLocal(LocalSampleProps())
  )


  override val job =  step1.execute()
}
