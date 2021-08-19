package examples.jobs

import etlflow.EtlStepList
import etlflow.etljobs.SequentialEtlJob
import etlflow.etlsteps.BQLoadStep
import etlflow.gcp.BQInputType
import examples.schema.MyEtlJobProps.EtlJob5Props

case class EtlJob5Definition(job_properties: EtlJob5Props) extends SequentialEtlJob[EtlJob5Props] {

//  private implicit val spark: SparkSession = SparkManager.createSparkSession(Set(LOCAL),hive_support = false)
//
//  private val step1 = SparkReadWriteStep[Rating](
//    name             = "LoadRatingsParquetToJdbc",
//    input_location   = job_properties.ratings_input_path,
//    input_type       = IOType.PARQUET,
//    output_type      = RDB(JDBC(config.db.url,config.db.user,config.db.password,config.db.driver)),
//    output_location  = job_properties.ratings_output_table,
//    output_save_mode = SaveMode.Overwrite
//  )

//  private val step2 = SparkReadWriteStep[RatingBQ](
//    name             = "LoadRatingsBqToJdbc",
//    input_location   = Seq("test.ratings"),
//    input_type       = IOType.BQ(),
//    output_type      = JDBC(config.dbLog.url,config.dbLog.user,config.dbLog.password,config.dbLog.driver),
//    output_location  = job_properties.ratings_output_table,
//    output_save_mode = SaveMode.Overwrite
//  )

  private val step2 = BQLoadStep(
    name           = "LoadQueryDataBQPar",
    input_location = Left(""),
    input_type     = BQInputType.BQ,
    output_dataset = "test",
    output_table   = "ratings_grouped_par"
  )
  val etlStepList = EtlStepList(step2)
}
