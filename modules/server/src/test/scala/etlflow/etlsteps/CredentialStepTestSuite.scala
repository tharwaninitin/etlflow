package etlflow.etlsteps

import etlflow.core.CoreEnv
import etlflow.crypto.{CryptoApi, CryptoEnv}
import etlflow.schema.Config
import etlflow.schema.Credential.JDBC
import io.circe.generic.auto._
import zio.ZIO
import zio.test.Assertion.equalTo
import zio.test._

case class CredentialStepTestSuite(config: Config) {

  val db_user_password = CryptoApi.encrypt(config.db.get.user)zip(CryptoApi.encrypt(config.db.get.password))

  val insert_credential_script = db_user_password.map(tp => s"""
      INSERT INTO credential (name,type,value) VALUES (
      'etlflow',
      'jdbc',
      '{"url": "${config.db.get.url}", "user": "${tp._1}", "password": "${tp._2}", "driver": "org.postgresql.Driver" }'
      )
      """)

  val cred_step = GetCredentialStep[JDBC](
    name = "GetCredential",
    credential_name = "etlflow",
  )

  val spec: ZSpec[environment.TestEnvironment with CoreEnv with CryptoEnv, Any] =
    suite("GetCredential Step")(
      testM("Execute GetCredentialStep") {
        def step1(script: String) = DBQueryStep(
          name = "AddCredential",
          query = script,
          credentials = config.db.get
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
