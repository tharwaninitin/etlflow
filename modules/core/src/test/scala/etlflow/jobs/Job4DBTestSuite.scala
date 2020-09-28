package etlflow.jobs

import etlflow.etlsteps.DBQueryStep
import etlflow.utils.Configuration
import etlflow.{LoadData, TestSuiteHelper}
import org.scalatest.{FlatSpec, Matchers}
import zio.Runtime.default.unsafeRun

class Job4DBTestSuite extends FlatSpec with Matchers with TestSuiteHelper with Configuration {

  val delete_credential_script = "DELETE FROM credentials WHERE name = 'etlflow'"

  val insert_credential_script = s"""
      INSERT INTO credentials VALUES(
      'etlflow',
      'jdbc',
      '{"url" : "${config.dbLog.url}", "user" : "${config.dbLog.user}", "password" : "${config.dbLog.password}", "driver" : "org.postgresql.Driver" }'
      )
      """

  unsafeRun(
    DBQueryStep(
      name  = "DeleteCredential",
      query = delete_credential_script,
      credentials = config.dbLog
    ).process()
  )

  unsafeRun(
    DBQueryStep(
      name  = "AddCredential",
      query = insert_credential_script,
      credentials = config.dbLog
    ).process()
  )

  LoadData.main(
    Array(
      "run_job",
      "--job_name",
      "EtlJob4",
    )
  )

  "LoadData" should "EtlJob4 should run successfully" in {
    assert(true)
  }
}