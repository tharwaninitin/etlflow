package examples.jobs

import com.google.cloud.bigquery.JobInfo
import etlflow.EtlStepList
import etlflow.etljobs.SequentialEtlJob
import etlflow.etlsteps.{BQLoadStep, EtlStep, SparkReadWriteStep}
import etlflow.spark.SparkManager
import etlflow.utils.Environment.LOCAL
import etlflow.utils.{CSV, ORC}
import examples.schema.MyEtlJobProps.EtlJob1Props
import examples.schema.MyEtlJobSchema.Rating
import org.apache.spark.sql.{SaveMode, SparkSession}
case class EtlJob0DefinitionDataproc(job_properties: EtlJob1Props) extends SequentialEtlJob[EtlJob1Props] {

  private val job_props = job_properties.asInstanceOf[EtlJob1Props]
  private implicit val spark: SparkSession = SparkManager.createSparkSession(Set(LOCAL))

  private val step1 = SparkReadWriteStep[Rating](
    name                      = "LoadRatingsParquet",
    input_location            = job_props.ratings_input_path,
    input_type                = CSV(",", true, "FAILFAST"),
    output_type               = ORC,
    output_location           = job_props.ratings_intermediate_path,
    output_repartitioning     = true,
    output_repartitioning_num = 1,
    output_save_mode          = SaveMode.Overwrite,
    output_filename           = job_props.ratings_output_file_name,
  )

  private val step2 = BQLoadStep(
    name                      = "LoadRatingBQ",
    input_location            = Left(job_props.ratings_intermediate_path + "/" + job_props.ratings_output_file_name.get),
    input_type                = ORC,
    output_dataset            = job_props.ratings_output_dataset,
    output_table              = job_props.ratings_output_table_name,
    output_create_disposition = JobInfo.CreateDisposition.CREATE_IF_NEEDED
  )

  val etlStepList: List[EtlStep[Unit, Unit]] = EtlStepList(step1,step2)
}
