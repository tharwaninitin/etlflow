package etlflow.etlsteps

import crypto4s.Crypto
import etlflow.db.DBEnv
import etlflow.model.Config
import etlflow.model.Credential.JDBC
import etlflow.json.CredentialImplicits._
import zio.ZIO
import zio.test.Assertion.equalTo
import zio.test._

case class CredentialStepTestSuite(config: Config) {
  val crypto      = Crypto(config.secretkey)
  val db_user     = crypto.encrypt(config.db.get.user)
  val db_password = crypto.encrypt(config.db.get.password)

  val insert_credential_script = s"""
      INSERT INTO credential (name,type,value) VALUES (
      'etlflow',
      'jdbc',
      '{"url": "${config.db.get.url}", "user": "$db_user", "password": "$db_password", "driver": "org.postgresql.Driver" }'
      )
      """

  val cred_step = GetCredentialStep[JDBC](
    name = "GetCredential",
    credential_name = "etlflow"
  )

  val spec: ZSpec[environment.TestEnvironment with DBEnv, Any] =
    suite("GetCredential Step")(
      testM("Execute GetCredentialStep") {
        def step1(script: String) = DBQueryStep(
          name = "AddCredential",
          query = script
        )
        val job = for {
          _ <- step1(insert_credential_script).process
          _ <- cred_step.process
        } yield ()
        assertM(job.foldM(ex => ZIO.fail(ex.getMessage), _ => ZIO.succeed("ok")))(equalTo("ok"))
      },
      test("Execute getStepProperties") {
        val props = cred_step.getStepProperties
        assertTrue(props == Map("credential_name" -> "etlflow"))
      }
    )
}
