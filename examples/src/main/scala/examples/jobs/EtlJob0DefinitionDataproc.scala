package examples.jobs

import com.google.cloud.bigquery.JobInfo
import etlflow.EtlStepList
import etlflow.etljobs.SequentialEtlJob
import etlflow.etlsteps.{BQLoadStep, EtlStep, SparkReadWriteStep}
import etlflow.gcp.BQInputType
import etlflow.spark.{IOType, SparkManager}
import examples.schema.MyEtlJobProps.EtlJob1Props
import examples.schema.MyEtlJobSchema.Rating
import org.apache.spark.sql.{SaveMode, SparkSession}

case class EtlJob0DefinitionDataproc(job_properties: EtlJob1Props) extends SequentialEtlJob[EtlJob1Props] {

  private implicit val spark: SparkSession = SparkManager.createSparkSession()

  private val step1 = SparkReadWriteStep[Rating](
    name                      = "LoadRatingsParquet",
    input_location            = job_properties.ratings_input_path,
    input_type                = IOType.CSV(",", true, "FAILFAST"),
    output_type               = IOType.ORC,
    output_location           = job_properties.ratings_intermediate_path,
    output_repartitioning     = true,
    output_repartitioning_num = 1,
    output_save_mode          = SaveMode.Overwrite,
    output_filename           = job_properties.ratings_output_file_name,
  )

  private val step2 = BQLoadStep(
    name                      = "LoadRatingBQ",
    input_location            = Left(job_properties.ratings_intermediate_path + "/" + job_properties.ratings_output_file_name.get),
    input_type                = BQInputType.ORC,
    output_dataset            = job_properties.ratings_output_dataset,
    output_table              = job_properties.ratings_output_table_name,
    output_create_disposition = JobInfo.CreateDisposition.CREATE_IF_NEEDED
  )

  val etlStepList: List[EtlStep[Unit, Unit]] = EtlStepList(step1,step2)
}
