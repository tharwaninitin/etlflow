package examples.jobs

import etlflow.etljobs.EtlJob
import etlflow.etlsteps.DPSparkJobStep
import etlflow.model.Executor.DATAPROC
import examples.schema.MyEtlJobProps.SampleProps

case class EtlJobDpSparkJobStep(job_properties: SampleProps) extends EtlJob[SampleProps] {

  val dpConfig = DATAPROC(
    sys.env("DP_PROJECT_ID"),
    sys.env("DP_REGION"),
    sys.env("DP_ENDPOINT"),
    sys.env("DP_CLUSTER_NAME")
  )
  val libs = sys.env("DP_LIBS").split(",").toList

  val step = DPSparkJobStep(
    name = "DPSparkJobStepExample",
    args = List.empty,
    config = dpConfig,
    main_class = sys.env("DP_MAIN_CLASS"),
    libs = libs
  )

  val job = step.execute

}
