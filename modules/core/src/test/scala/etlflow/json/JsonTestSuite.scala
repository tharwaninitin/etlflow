package etlflow.json

import etlflow.json.Schema._
import zio.test.Assertion.equalTo
import zio.test._
import scala.collection.immutable.ListMap

object JsonTestSuite extends Implicits {
  val student1Json: String = """{
                               |"name":"John",
                               |"id":"63",
                               |"class": "101"
                               |}""".stripMargin

  val student2Json: String = """{
                               |"name":"John",
                               |"id":"63"
                               |}""".stripMargin

  val ip1: EtlJob23Props = EtlJob23Props("data/movies/ratings/*", "test", "ratings_par", true, true, LoggingLevel.DEBUG)
  val op1: String =
    """{"ratings_input_path":"data/movies/ratings/*","ratings_output_dataset":"test","ratings_output_table_name":"ratings_par","job_send_slack_notification":true,"job_enable_db_logging":true,"job_notification_level":"debug"}""".stripMargin

  val ip2: Map[String, String] = ListMap(
    Map(
      "job_send_slack_notification" -> "false",
      "job_enable_db_logging"       -> "true",
      "job_notification_level"      -> "info",
      "job_max_active_runs"         -> "10",
      "job_name"                    -> "etlflow.coretests.jobs.Job3DBSteps",
      "job_description"             -> "",
      "job_props_name"              -> "etlflow.coretests.Schema$EtlJob4Props",
      "job_deploy_mode"             -> "dataproc",
      "job_retry_delay_in_minutes"  -> "0",
      "job_schedule"                -> "0 30 7 ? * *",
      "job_retries"                 -> "0"
    ).toSeq.sortBy(_._2): _*
  )

  val op2 =
    """{"job_description":"","job_retry_delay_in_minutes":"0","job_retries":"0","job_schedule":"0 30 7 ? * *","job_max_active_runs":"10","job_deploy_mode":"dataproc","job_props_name":"etlflow.coretests.Schema$EtlJob4Props","job_name":"etlflow.coretests.jobs.Job3DBSteps","job_send_slack_notification":"false","job_notification_level":"info","job_enable_db_logging":"true"}""".stripMargin

  val spec: Spec[JSON, Any] =
    suite("Json Test")(
      test("convertToObject: String to Student") {
        val ip = JSON.convertToObjectM[Student](student1Json)
        assertZIO(ip)(equalTo(Student("63", "John", Some("101"))))
      },
      test("convertToObject: String to Student") {
        val ip = JSON.convertToObjectM[Student](student2Json)
        assertZIO(ip)(equalTo(Student("63", "John", None)))
      },
      test("convertToObject: String to Map") {
        val ip = JSON.convertToObjectM[Map[String, String]](student2Json)
        assertZIO(ip)(equalTo(Map("name" -> "John", "id" -> "63")))
      },
      test("convertToString: Case class to String") {
        val ip = JSON.convertToStringM(ip1)
        assertZIO(ip)(equalTo(op1))
      },
      test("convertToString: Map to String") {
        val ip = JSON.convertToStringM[Map[String, String]](ip2)
        assertZIO(ip)(equalTo(op2))
      }
    )
}
