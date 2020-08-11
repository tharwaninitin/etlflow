package examples.jobs

import etlflow.EtlStepList
import etlflow.etljobs.SequentialEtlJob
import etlflow.etlsteps.{DPSparkJobStep, EtlStep}
import etlflow.utils.Config
import etlflow.utils.Executor.DATAPROC
import examples.schema.MyEtlJobProps

case class EtlJob1Trigger(job_properties: MyEtlJobProps, globalProperties: Config) extends SequentialEtlJob {
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
