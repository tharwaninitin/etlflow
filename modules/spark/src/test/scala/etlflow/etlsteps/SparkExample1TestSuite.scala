//package etlflow.etlsteps
//
//import etlflow.TestSparkSession
//import etlflow.coretests.Schema._
//import etlflow.coretests.TestSuiteHelper
//import etlflow.db.DBApi
//import etlflow.model.Credential.JDBC
//import etlflow.spark.IOType.{PARQUET, RDB}
//import etlflow.spark.ReadApi
//import org.apache.spark.sql.{Dataset, Row, SaveMode}
//import zio.Runtime.default.unsafeRun
//import zio.ZIO
//import zio.test.Assertion.equalTo
//import zio.test.{DefaultRunnableSpec, ZSpec, assert}
//
//object SparkExample1TestSuite extends DefaultRunnableSpec with TestSuiteHelper with TestSparkSession {
//
//  // STEP 1: Define step
//  // Note: Here Parquet file has 6 columns and Rating Case Class has 4 out of those 6 columns so only 4 will be selected
//  val input_path_parquet  = s"$canonical_path/modules/core/src/test/resources/input/movies/ratings_parquet"
//  val output_table        = "ratings"
//
//  val step1 = SparkReadWriteStep[Rating](
//    name             = "LoadRatingsParquetToJdbc",
//    input_location   = Seq(input_path_parquet),
//    input_type       = PARQUET,
//    output_type      = RDB(JDBC(credentials.url, credentials.user, credentials.password, "org.postgresql.Driver")),
//    output_location  = "ratings",
//    output_save_mode = SaveMode.Overwrite
//  )
//
//  val step2 = SparkReadStep[Rating](
//    name             = "LoadRatingsParquet",
//    input_location   = Seq(input_path_parquet),
//    input_type       = PARQUET,
//  )
//
//  // STEP 2: Run Step
//  unsafeRun(step1.process(()))
//  val op: Dataset[Rating] = unsafeRun(step2.process(()))
//  op.show(10)
//
//  // STEP 3: Run Test
//  val raw: Dataset[Rating] = ReadApi.LoadDS[Rating](Seq(input_path_parquet), PARQUET)(spark)
//  val Row(sum_ratings: Double, count_ratings: Long) = raw.selectExpr("sum(rating)","count(*)").first()
//  val query: String = s"SELECT sum(rating) sum_ratings, count(*) as count FROM $output_table"
//  val db_task: ZIO[zio.ZEnv, Throwable, List[RatingsMetrics]] =
//    DBApi
//      .executeQueryListOutput[RatingsMetrics](s"SELECT sum(rating) sum_ratings, count(*) as count FROM $output_table")(rs => RatingsMetrics(rs.double("sum_ratings"),rs.long("count_ratings")))
//      .provideCustomLayer(etlflow.db.liveDB(credentials))
//  val db_metrics: RatingsMetrics = unsafeRun(db_task).head
//
//  override def spec: ZSpec[_root_.zio.test.environment.TestEnvironment, Any] = {
//    suite("Spark Step Test Suite")(
//      test("Record counts should be matching in transformed DF and DB table") {
//        assert(count_ratings)(equalTo(db_metrics.count_ratings))
//      },
//      test("Sum of ratings should be matching in transformed DF and DB table ") {
//        assert(sum_ratings)(equalTo(db_metrics.sum_ratings))
//      }
//    )
//  }
//}
//
//
