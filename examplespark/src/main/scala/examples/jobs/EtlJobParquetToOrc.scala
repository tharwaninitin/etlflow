package examples.jobs

import etlflow.EtlStepList
import etlflow.etljobs.SequentialEtlJob
import etlflow.etlsteps.{EtlStep, SparkReadWriteStep}
import etlflow.spark.{IOType, SparkManager}
import examples.schema.MyEtlJobProps.EtlJob1Props
import examples.schema.MyEtlJobSchema.Rating
import org.apache.spark.sql.SaveMode
import etlflow.spark.Environment.LOCAL

case class EtlJobParquetToOrc(job_properties: EtlJob1Props) extends SequentialEtlJob[EtlJob1Props] {

  private implicit val spark = SparkManager.createSparkSession(Set(LOCAL), hive_support = false)

  private val step1 = SparkReadWriteStep[Rating](
    name                      = "LoadRatingsParquet",
    input_location            = job_properties.ratings_input_path,
    input_type                = IOType.PARQUET,
    output_type               = IOType.ORC,
    output_location           = job_properties.ratings_intermediate_path,
    output_repartitioning     = true,
    output_repartitioning_num = 1,
    output_save_mode          = SaveMode.Overwrite,
    output_filename           = job_properties.ratings_output_file_name,
  )

  val etlStepList: List[EtlStep[Unit, Unit]] = EtlStepList(step1)
}
