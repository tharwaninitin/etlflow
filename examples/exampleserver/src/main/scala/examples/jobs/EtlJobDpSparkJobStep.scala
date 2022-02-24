package examples.jobs

import etlflow.etljobs.EtlJob
import etlflow.etlsteps.DPSparkJobStep
import etlflow.log.LogEnv
import examples.schema.MyEtlJobProps.SampleProps
import gcp4zio.DPJob

case class EtlJobDpSparkJobStep(job_properties: SampleProps) extends EtlJob[SampleProps] {

  val libs = sys.env("DP_LIBS").split(",").toList

  val step = DPSparkJobStep(
    name = "DPSparkJobStepExample",
    args = List.empty,
    mainClass = sys.env("DP_MAIN_CLASS"),
    libs = libs,
    conf = Map.empty,
    sys.env("DP_CLUSTER_NAME"),
    sys.env("DP_PROJECT_ID"),
    sys.env("DP_REGION")
  )

  val job = step.execute.unit.provideSomeLayer[LogEnv](DPJob.live(sys.env("DP_ENDPOINT")))
}
