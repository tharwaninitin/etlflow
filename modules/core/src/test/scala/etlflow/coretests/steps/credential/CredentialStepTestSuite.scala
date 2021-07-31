package etlflow.coretests.steps.credential

import etlflow.coretests.TestSuiteHelper
import etlflow.etlsteps.{DBQueryStep, GetCredentialStep}
import etlflow.schema.Credential.JDBC
import etlflow.utils.CredentialImplicits._
import zio.ZIO
import zio.test.Assertion.equalTo
import zio.test.{DefaultRunnableSpec, ZSpec, assertM, environment}

object CredentialStepTestSuite extends DefaultRunnableSpec with TestSuiteHelper {

  val dbLog_user = enc.encrypt(config.db.user)
  val dbLog_password = enc.encrypt(config.db.password)

  val insert_credential_script = s"""
      INSERT INTO credential (name,type,value) VALUES(
      'etlflow',
      'jdbc',
      '{"url" : "${config.db.url}", "user" : "${dbLog_user}", "password" : "${dbLog_password}", "driver" : "org.postgresql.Driver" }'
      )
      """

  def spec: ZSpec[environment.TestEnvironment, Any] =
    suite("EtlFlow")(
      suite("GetCredential Step")(
        testM("Execute GetCredential step") {
          val step1 =  DBQueryStep(
            name  = "AddCredential",
            query = insert_credential_script,
            credentials = config.db
          )
          val step2 =  GetCredentialStep[JDBC](
            name  = "GetCredential",
            credential_name = "etlflow",
          )

          val job = for {
            _ <- step1.process().provideCustomLayer(fullLayer)
            _ <- step2.process().provideCustomLayer(fullLayer)
          } yield ()

          assertM(job.foldM(ex => ZIO.fail(ex.getMessage), _ => ZIO.succeed("ok")))(equalTo("ok"))
        }
      )
    )
}


