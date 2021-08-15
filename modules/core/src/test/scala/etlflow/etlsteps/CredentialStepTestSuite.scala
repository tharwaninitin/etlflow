package etlflow.etlsteps

import etlflow.CoreEnv
import etlflow.crypto.CryptoApi
import etlflow.schema.Config
import etlflow.schema.Credential.JDBC
import io.circe.generic.auto._
import zio.ZIO
import zio.test.Assertion.equalTo
import zio.test._

case class CredentialStepTestSuite(config: Config) {

  val db_user_password = CryptoApi.encrypt(config.db.user)zip(CryptoApi.encrypt(config.db.password))

  val insert_credential_script = db_user_password.map(tp => s"""
      INSERT INTO credential (name,type,value) VALUES (
      'etlflow',
      'jdbc',
      '{"url": "${config.db.url}", "user": "${tp._1}", "password": "${tp._2}", "driver": "org.postgresql.Driver" }'
      )
      """)

  val cred_step = GetCredentialStep[JDBC](
    name = "GetCredential",
    credential_name = "etlflow",
  )

  val spec: ZSpec[environment.TestEnvironment with CoreEnv, Any] =
    suite("GetCredential Step")(
      testM("Execute GetCredentialStep") {
        def step1(script: String) = DBQueryStep(
          name = "AddCredential",
          query = script,
          credentials = config.db
        )
        val job = for {
          script  <- insert_credential_script
          _       <- step1(script).process(())
          _       <- cred_step.process(())
        } yield ()
        assertM(job.foldM(ex => ZIO.fail(ex.getMessage), _ => ZIO.succeed("ok")))(equalTo("ok"))
      },
      test("Execute getStepProperties") {
        val props = cred_step.getStepProperties()
        assert(props)(equalTo(Map("credential_name" -> "etlflow")))
      }
    )
}
