package etlflow.jobs

import com.google.cloud.bigquery.JobInfo
import etlflow.EtlStepList
import etlflow.Schema.{EtlJob1Props, Rating}
import etlflow.etljobs.SequentialEtlJobWithLogging
import etlflow.etlsteps.{BQLoadStep, EtlStep, SparkReadWriteStep}
import etlflow.utils.{GlobalProperties, ORC, PARQUET}
import org.apache.spark.sql.SaveMode

case class EtlJob1Definition(job_properties: EtlJob1Props, global_properties: Option[GlobalProperties]) extends SequentialEtlJobWithLogging {

  val step1 = SparkReadWriteStep[Rating](
    name              = "LoadRatingsParquet",
    input_location    = job_properties.ratings_input_path,
    input_type        = PARQUET,
    output_type       = ORC,
    output_location   = job_properties.ratings_intermediate_bucket,
    output_save_mode  = SaveMode.Overwrite,
    output_filename   = job_properties.ratings_output_file_name
  )

  val step2 = BQLoadStep(
    name                      = "LoadRatingBQ",
    input_location            = Left(job_properties.ratings_intermediate_bucket + "/" + job_properties.ratings_output_file_name.get),
    input_type                = ORC,
    output_dataset            = job_properties.ratings_output_dataset,
    output_table              = job_properties.ratings_output_table_name,
    output_create_disposition = JobInfo.CreateDisposition.CREATE_IF_NEEDED
  )

  val etlStepList: List[EtlStep[_, _]] = EtlStepList(step1,step2)
}
