package examples.jobs

import etlflow.EtlStepList
import etlflow.etljobs.SequentialEtlJob
import etlflow.etlsteps.{DPSparkJobStep, EtlStep}
import examples.schema.MyEtlJobProps.SampleProps

case class EtlJob1DefinitionLocal(job_properties: SampleProps) extends SequentialEtlJob[SampleProps] {

  private val job_props = job_properties.asInstanceOf[SampleProps]
  val dpConfig = DATAPROC(
    sys.env("DP_PROJECT_ID"),
    sys.env("DP_REGION"),
    sys.env("DP_ENDPOINT"),
    sys.env("DP_CLUSTER_NAME")
  )
  val libs = sys.env("DP_LIBS").split(",").toList

  val step = DPSparkJobStep(
    name        = "DPSparkJobStepExample",
    job_name    = sys.env("DP_JOB_NAME"),
    props       = Map.empty,
    config      = dpConfig,
    main_class  = sys.env("DP_MAIN_CLASS"),
    libs        = libs
  )
  override def etlStepList: List[EtlStep[Unit, Unit]] = EtlStepList(step)

}
