package etlflow.jobs

import etlflow.EtlStepList
import etlflow.coretests.Schema.EtlJobDeltaLake
import etlflow.etljobs.SequentialEtlJob
import etlflow.etlsteps.DeltaLakeStep.DeltaLakeCmd
import etlflow.etlsteps.{DeltaLakeStep, EtlStep}
import etlflow.spark.Environment.LOCAL
import etlflow.spark.SparkManager
import org.apache.spark.sql.SparkSession


case class Job4SparkDeltaStep(job_properties: EtlJobDeltaLake) extends SequentialEtlJob[EtlJobDeltaLake] {

  private implicit val spark: SparkSession = SparkManager.createSparkSession(Set(LOCAL),hive_support = false)

  val step1 = DeltaLakeStep(
    name         = "Vacuum the delta lake table",
    command      = DeltaLakeCmd.VACCUME(job_properties.input_path,0.000001),
  )

  override def etlStepList: List[EtlStep[Unit, Unit]] = EtlStepList(step1)
}

