package examples.jobs

import com.google.cloud.bigquery.JobInfo
import etlflow.EtlStepList
import etlflow.etljobs.SequentialEtlJob
import etlflow.etlsteps.{BQLoadStep, EtlStep}
import etlflow.gcp.BQInputType
import examples.schema.MyEtlJobProps.EtlJob1Props

case class EtlJobBqLoadStep(job_properties: EtlJob1Props) extends SequentialEtlJob[EtlJob1Props] {

  private val step1 = BQLoadStep(
    name                      = "LoadRatingBQ",
    input_location            = Left(job_properties.ratings_input_path + "/" + job_properties.ratings_output_file_name.get),
    input_type                = BQInputType.PARQUET,
    output_dataset            = job_properties.ratings_output_dataset,
    output_table              = job_properties.ratings_output_table_name,
    output_create_disposition = JobInfo.CreateDisposition.CREATE_IF_NEEDED
  )

  val etlStepList: List[EtlStep[Unit, Unit]] = EtlStepList(step1)
}