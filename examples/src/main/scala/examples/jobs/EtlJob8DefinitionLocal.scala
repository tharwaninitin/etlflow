package examples.jobs

import etlflow.etljobs.GenericEtlJob
import etlflow.etlsteps.SparkReadWriteStep
import etlflow.spark.SparkManager
import etlflow.utils.Environment.{GCP, LOCAL}
import etlflow.utils.{BQ, JDBC}
import examples.schema.MyEtlJobProps.{EtlJob1Props, EtlJob23Props}
import examples.schema.MyEtlJobSchema.Rating
import org.apache.spark.sql.{SaveMode, SparkSession}

case class EtlJob8DefinitionLocal(job_properties: EtlJob23Props) extends GenericEtlJob[EtlJob23Props] {

  private implicit val spark: SparkSession = SparkManager.createSparkSession(Set(LOCAL),hive_support = false)

  val query = """ SELECT * FROM `test.ratings`""".stripMargin

  private val step1 = SparkReadWriteStep[Rating](
    name                      = "LoadRatingsParquet",
    input_location            = Seq(query),
    input_type                = BQ(temp_dataset =  "test",operation_type =  "query"),
    output_type               = config.dbLog,
    output_location           = job_properties.ratings_output_table_name,
    output_save_mode          = SaveMode.Overwrite
  )

  private val step2 = SparkReadWriteStep[Rating](
    name                      = "LoadRatingsParquet",
    input_location            = Seq("test.ratings"),
    input_type                = BQ(),
    output_type               = config.dbLog,
    output_location           = job_properties.ratings_output_table_name,
    output_save_mode          = SaveMode.Overwrite
  )


  val job =
    for {
      _   <- step1.execute()
      _   <- step2.execute()
    } yield ()
}
