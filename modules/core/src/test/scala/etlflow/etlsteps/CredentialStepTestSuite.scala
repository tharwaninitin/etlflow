package etlflow.etlsteps

import etlflow.coretests.TestSuiteHelper
import etlflow.crypto.CryptoApi
import etlflow.schema.Credential.JDBC
import zio.ZIO
import zio.test.Assertion.equalTo
import zio.test._
import zio.Runtime.default.unsafeRun
import etlflow.utils.CredentialImplicits._

object CredentialStepTestSuite extends DefaultRunnableSpec with TestSuiteHelper {

  val dbLog_user = CryptoApi.encrypt(config.db.user).provideCustomLayer(cryptoLayer)
  val dbLog_password = CryptoApi.encrypt(config.db.password).provideCustomLayer(cryptoLayer)

  val insert_credential_script =
    s"""
      INSERT INTO credential (name,type,value) VALUES(
      'etlflow',
      'jdbc',
      '{"url" : "${config.db.url}", "user" : "${unsafeRun(dbLog_user)}", "password" : "${unsafeRun(dbLog_password)}", "driver" : "org.postgresql.Driver" }'
      )
      """

  val step2 = GetCredentialStep[JDBC](
    name = "GetCredential",
    credential_name = "etlflow",
  )

  def spec: ZSpec[environment.TestEnvironment, Any] =
    suite("GetCredential Step")(
      testM("Execute GetCredential step") {
        val step1 = DBQueryStep(
          name = "AddCredential",
          query = insert_credential_script,
          credentials = config.db
        )
        val step2 = GetCredentialStep[JDBC](
          name = "GetCredential",
          credential_name = "etlflow",
        )

        val job = for {
          _ <- step1.process().provideCustomLayer(fullLayer)
          _ <- step2.process().provideCustomLayer(fullLayer)
        } yield ()

        assertM(job.foldM(ex => ZIO.fail(ex.getMessage), _ => ZIO.succeed("ok")))(equalTo("ok"))
      },
      test("Execute getStepProperties") {
        val props = step2.getStepProperties()
        assert(props)(equalTo(Map("credential_name" -> "etlflow")))
      }
    )
}