package etlflow.etlsteps

import etlflow.TestSparkSession
import etlflow.schema.{Rating, RatingBQ, RatingOutput, RatingOutputCsv}
import etlflow.spark.IOType.{BQ, CSV, JSON, PARQUET}
import etlflow.spark.{SparkEnv, SparkUDF}
import org.apache.spark.sql.functions.{col, from_unixtime}
import org.apache.spark.sql.types.{DateType, IntegerType}
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}
import zio.ZIO
import zio.test.Assertion.equalTo
import zio.test._

object SparkExample2TestSuite extends TestSparkSession with SparkUDF {

  case class EtlJob6Props(
      ratings_input_dataset: String = "test",
      ratings_input_table_name: String = "ratings",
      ratings_intermediate_bucket: String = s"gs://${sys.env("GCS_BUCKET")}/intermediate/ratings",
      ratings_output_bucket_1: String = s"gs://${sys.env("GCS_BUCKET")}/output/ratings/csv",
      ratings_output_bucket_2: String = s"s3a://${sys.env("S3_BUCKET")}/temp/output/ratings/parquet",
      ratings_output_bucket_3: String = s"gs://${sys.env("GCS_BUCKET")}/output/ratings/json",
      ratings_output_file_name: Option[String] = Some("ratings.csv")
  )

  val job_props: EtlJob6Props = EtlJob6Props()

  val partition_date_col = "date_int"

  val query = s""" SELECT * FROM `${job_props.ratings_input_dataset}.${job_props.ratings_input_table_name}` """.stripMargin

  val step0 = SparkReadWriteStep[Rating, Rating](
    name = "LoadRatings BQ(query) to GCS CSV",
    input_location = Seq(query),
    input_type = BQ(temp_dataset = "test", operation_type = "query"),
    output_type = CSV(),
    output_location = job_props.ratings_intermediate_bucket,
    output_save_mode = SaveMode.Overwrite,
    output_repartitioning = true,
    output_repartitioning_num = 3
  )

  val step1 = SparkReadWriteStep[RatingBQ, RatingBQ](
    name = "LoadRatings BQ(table) to GCS CSV",
    input_location = Seq(job_props.ratings_input_dataset + "." + job_props.ratings_input_table_name),
    input_type = BQ(),
    output_type = CSV(),
    output_location = job_props.ratings_intermediate_bucket,
    output_save_mode = SaveMode.Overwrite,
    output_repartitioning = true,
    output_repartitioning_num = 3
  )

  def enrichRatingCsvData(spark: SparkSession, in: Dataset[Rating]): Dataset[RatingOutputCsv] = {
    import spark.implicits._

    // val mapping = Encoders.product[RatingOutputCsv]

    val ratings_df = in
      .withColumnRenamed("user_id", "User Id")
      .withColumnRenamed("movie_id", "Movie Id")
      .withColumnRenamed("rating", "Ratings")
      .withColumn("date", from_unixtime(col("timestamp"), "yyyy-MM-dd").cast(DateType))
      .withColumnRenamed("date", "Movie Date")

    ratings_df.as[RatingOutputCsv]
  }

  val step21 = SparkReadWriteStep[Rating, RatingOutputCsv](
    name = "LoadRatings GCS Csv To GCS Csv",
    input_location = Seq(job_props.ratings_intermediate_bucket),
    input_type = CSV(),
    transform_function = Some(enrichRatingCsvData),
    output_type = CSV(),
    output_location = job_props.ratings_output_bucket_1,
    output_save_mode = SaveMode.Overwrite,
    output_repartitioning = true,
    output_repartitioning_num = 1,
    output_filename = job_props.ratings_output_file_name
  )

  def enrichRatingData(spark: SparkSession, in: Dataset[Rating]): Dataset[RatingOutput] = {
    import spark.implicits._

    val ratings_df = in
      .withColumn("date", from_unixtime(col("timestamp"), "yyyy-MM-dd").cast(DateType))
      .withColumn(partition_date_col, get_formatted_date("date", "yyyy-MM-dd", "yyyyMM").cast(IntegerType))
      .where(f"$partition_date_col in ('201601', '201512', '201510')")

    ratings_df.as[RatingOutput]
  }

  val step22 = SparkReadWriteStep[Rating, RatingOutput](
    name = "LoadRatings GCS Csv To S3 Parquet",
    input_location = Seq(job_props.ratings_intermediate_bucket),
    input_type = CSV(),
    transform_function = Some(enrichRatingData),
    output_type = PARQUET,
    output_location = job_props.ratings_output_bucket_2,
    output_save_mode = SaveMode.Overwrite,
    output_partition_col = Seq(s"$partition_date_col")
  )

  val step3 = SparkReadWriteStep[Rating, RatingOutput](
    name = "LoadRatings GCS Csv To GCS Json",
    input_location = Seq(job_props.ratings_intermediate_bucket),
    input_type = CSV(),
    transform_function = Some(enrichRatingData),
    output_type = JSON(),
    output_location = job_props.ratings_output_bucket_3,
    output_save_mode = SaveMode.Overwrite,
    output_partition_col = Seq(s"$partition_date_col"),
    output_repartitioning = true,
    output_repartitioning_num = 1
  )

  val job: ZIO[SparkEnv, Throwable, Unit] = for {
    _ <- step1.process
    _ <- step21.process.zipPar(step22.process)
    _ <- step3.process
  } yield ()

  val spec: ZSpec[environment.TestEnvironment with SparkEnv, Any] =
    suite("Spark Steps 2")(testM("Execute SparkReadWriteStep steps") {
      assertM(job.foldM(ex => ZIO.fail(ex.getMessage), _ => ZIO.succeed("ok")))(equalTo("ok"))
    })
}
