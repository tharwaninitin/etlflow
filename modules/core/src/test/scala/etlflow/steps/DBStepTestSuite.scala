package etlflow.steps

import etlflow.TestSuiteHelper
import etlflow.etlsteps.DBQueryStep
import etlflow.utils.JDBC
import org.scalatest.{FlatSpec, Matchers}
import org.testcontainers.containers.PostgreSQLContainer

class DBStepTestSuite extends FlatSpec with Matchers with TestSuiteHelper {

  // STEP 1: Setup test containers
  val container = new PostgreSQLContainer("postgres:latest")
  container.start()

  val create_table_script = """
      CREATE TABLE ratings_par (
        user_id int
      , movie_id int
      , rating int
      , timestamp int
      , date date
      )
      """

  val step1 = DBQueryStep(
    name  = "UpdatePG",
    query = create_table_script,
    credentials = JDBC(container.getJdbcUrl, container.getUsername, container.getPassword, global_props.log_db_driver)
  )

  val step2 = DBQueryStep(
    name  = "UpdatePG",
    query = "BEGIN; DELETE FROM ratings_par WHERE 1 = 1; COMMIT;",
    credentials = JDBC(container.getJdbcUrl, container.getUsername, container.getPassword, global_props.log_db_driver)
  )

  runtime.unsafeRun(step1.process())
  runtime.unsafeRun(step2.process())
  // STEP 1: Initialize job properties and create BQ tables required for jobs
//  private val canonical_path = new java.io.File(".").getCanonicalPath
//  val global_props = new MyGlobalProperties(canonical_path + "/etljobs/src/test/resources/loaddata.properties")
//
//  val job_props = EtlJob23Props(
//    job_properties = Map.empty,
//    ratings_input_path = f"$canonical_path/etljobs/src/test/resources/input/movies/ratings/*",
//    ratings_output_dataset = "test",
//    ratings_output_table_name = "ratings_par"
//  )
//
//  // STEP 2: Execute JOB
//  val etljob = new EtlJob3Definition(EtlJob3CSVtoCSVtoBQGcsWith2Steps.toString, job_properties=job_props, global_properties=Some(global_props))
//
//  val thrown = intercept[Exception] {
//    etljob.execute
//  }
//
//  assert(thrown.getMessage === "Could not load data in FormatOptions{format=PARQUET} format in table test.ratings_par$20160102 due to error Error while reading data, error message: Input file is not in Parquet format.")
}


