package etlflow.jobs

import etlflow.coretests.Schema._
import etlflow.etljobs.SequentialEtlJob
import etlflow.etlsteps.{EtlStep, ParallelETLStep, SparkReadTransformWriteStep, SparkReadWriteStep}
import etlflow.spark.SparkUDF
import etlflow.spark.IOType.{BQ, CSV, JSON, PARQUET}
import etlflow.{EtlStepList, TestSparkSession}
import org.apache.spark.sql.functions.{col, from_unixtime}
import org.apache.spark.sql.types.{DateType, IntegerType}
import org.apache.spark.sql.{Dataset, Encoders, SaveMode, SparkSession}

case class Job1SparkS3andGCSandBQSteps(job_properties: EtlJob6Props)
  extends SequentialEtlJob[EtlJob6Props] with SparkUDF with TestSparkSession {

  val job_props: EtlJob6Props = job_properties
  val partition_date_col  = "date_int"

  val query = s""" SELECT * FROM `${job_props.ratings_input_dataset}.${job_props.ratings_input_table_name}` """.stripMargin

  val step0 = SparkReadWriteStep[Rating](
    name                      = "LoadRatings BQ(query) to GCS CSV",
    input_location            = Seq(query),
    input_type                = BQ(temp_dataset = "test", operation_type =  "query"),
    output_type               = CSV(),
    output_location           = job_props.ratings_intermediate_bucket,
    output_save_mode          = SaveMode.Overwrite,
    output_repartitioning     = true,
    output_repartitioning_num = 3,
  )

  val step1 = SparkReadWriteStep[RatingBQ](
    name                      = "LoadRatings BQ(table) to GCS CSV",
    input_location            = Seq(job_props.ratings_input_dataset + "." + job_props.ratings_input_table_name),
    input_type                = BQ(),
    output_type               = CSV(),
    output_location           = job_props.ratings_intermediate_bucket,
    output_save_mode          = SaveMode.Overwrite,
    output_repartitioning     = true,
    output_repartitioning_num = 3,
  )

  def enrichRatingCsvData(spark: SparkSession, in: Dataset[Rating]): Dataset[RatingOutputCsv] = {
    val mapping = Encoders.product[RatingOutputCsv]

    val ratings_df = in
      .withColumnRenamed("user_id","User Id")
      .withColumnRenamed("movie_id","Movie Id")
      .withColumnRenamed("rating","Ratings")
      .withColumn("date", from_unixtime(col("timestamp"), "yyyy-MM-dd").cast(DateType))
      .withColumnRenamed("date","Movie Date")

    ratings_df.as[RatingOutputCsv](mapping)
  }

  val step21 = SparkReadTransformWriteStep[Rating, RatingOutputCsv](
    name                      = "LoadRatings GCS Csv To GCS Csv",
    input_location            = Seq(job_props.ratings_intermediate_bucket),
    input_type                = CSV(),
    transform_function        = enrichRatingCsvData,
    output_type               = CSV(),
    output_location           = job_props.ratings_output_bucket_1,
    output_save_mode          = SaveMode.Overwrite,
    output_repartitioning     = true,
    output_repartitioning_num = 1,
    output_filename           = job_props.ratings_output_file_name,
  )

  def enrichRatingData(spark: SparkSession, in: Dataset[Rating]): Dataset[RatingOutput] = {
    val mapping = Encoders.product[RatingOutput]

    val ratings_df = in
      .withColumn("date", from_unixtime(col("timestamp"), "yyyy-MM-dd").cast(DateType))
      .withColumn(partition_date_col, get_formatted_date("date","yyyy-MM-dd","yyyyMM").cast(IntegerType))
      .where(f"$partition_date_col in ('201601', '201512', '201510')")

    ratings_df.as[RatingOutput](mapping)
  }

  val step22 = SparkReadTransformWriteStep[Rating, RatingOutput](
    name                 = "LoadRatings GCS Csv To S3 Parquet",
    input_location       = Seq(job_props.ratings_intermediate_bucket),
    input_type           = CSV(),
    transform_function   = enrichRatingData,
    output_type          = PARQUET,
    output_location      = job_props.ratings_output_bucket_2,
    output_save_mode     = SaveMode.Overwrite,
    output_partition_col = Seq(s"$partition_date_col"),
  )

  val step3 = SparkReadTransformWriteStep[Rating, RatingOutput](
    name                      = "LoadRatings GCS Csv To GCS Json",
    input_location            = Seq(job_props.ratings_intermediate_bucket),
    input_type                = CSV(),
    transform_function        = enrichRatingData,
    output_type               = JSON(),
    output_location           = job_props.ratings_output_bucket_3,
    output_save_mode          = SaveMode.Overwrite,
    output_partition_col      = Seq(s"$partition_date_col"),
    output_repartitioning     = true,
    output_repartitioning_num = 1,
  )

  val parstep = ParallelETLStep("ParallelStep")(step21,step22)

  val etlStepList: List[EtlStep[Unit, Unit]] = EtlStepList(step1,parstep,step3)
}
