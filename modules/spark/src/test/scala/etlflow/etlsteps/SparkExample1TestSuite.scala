package etlflow.etlsteps

import etlflow.model.Credential.JDBC
import etlflow.schema.{Rating, RatingsMetrics}
import etlflow.spark.IOType.{PARQUET, RDB}
import etlflow.spark.SparkEnv
import etlflow.utils.ApplicationLogger
import org.apache.spark.sql.{Dataset, Encoders, SaveMode}
import zio.test.Assertion.equalTo
import zio.test._
import zio.{RIO, ZIO}

object SparkExample1TestSuite extends ApplicationLogger {

  // Note: Here Parquet file has 6 columns and Rating Case Class has 4 out of those 6 columns so only 4 will be selected
  val canonical_path: String = new java.io.File(".").getCanonicalPath
  val input_path_parquet     = s"$canonical_path/modules/spark/src/test/resources/input/ratings_parquet"
  val output_table           = "ratings"
  val cred: JDBC             = JDBC("jdbc:postgresql://localhost:5432/etlflow", "etlflow", "etlflow", "org.postgresql.Driver")
  val jdbc: RDB              = RDB(cred)

  val step1: RIO[SparkEnv, Unit] = SparkReadWriteStep[Rating, Rating](
    name = "LoadRatingsParquetToJdbc",
    input_location = Seq(input_path_parquet),
    input_type = PARQUET,
    output_type = jdbc,
    output_location = output_table,
    output_save_mode = SaveMode.Overwrite
  ).process

  val step2: RIO[SparkEnv, Dataset[Rating]] = SparkReadStep[Rating, Rating](
    name = "LoadRatingsParquet",
    input_location = Seq(input_path_parquet),
    input_type = PARQUET
  ).process

  def step3(query: String): RIO[SparkEnv, Dataset[RatingsMetrics]] = SparkReadStep[RatingsMetrics, RatingsMetrics](
    name = "LoadRatingsDB",
    input_location = Seq(query),
    input_type = jdbc
  ).process

  val job: ZIO[SparkEnv, Throwable, Boolean] = for {
    _     <- step1
    ip_ds <- step2
    enc   = Encoders.product[RatingsMetrics]
    ip    = ip_ds.selectExpr("sum(rating) as sum_ratings", "count(*) as count_ratings").as[RatingsMetrics](enc).first()
    query = s"(SELECT sum(rating) as sum_ratings, count(*) as count_ratings FROM $output_table) as T"
    op_ds <- step3(query)
    op   = op_ds.first()
    _    = logger.info(s"IP => $ip")
    _    = logger.info(s"OP => $op")
    bool = ip == op
  } yield bool

  val spec: ZSpec[environment.TestEnvironment with SparkEnv, Any] =
    suite("Spark Steps 1")(
      testM("Record counts and Sum should be matching") {
        assertM(job.foldM(ex => ZIO.fail(ex.getMessage), op => ZIO.succeed(op)))(equalTo(true))
      }
    )
}
