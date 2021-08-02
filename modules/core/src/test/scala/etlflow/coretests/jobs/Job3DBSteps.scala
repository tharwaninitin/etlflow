package etlflow.coretests.jobs

import etlflow.coretests.Schema.{EtlJob4Props, EtlJobRun}
import etlflow.coretests.TestSuiteHelper
import etlflow.coretests.steps.credential.CredentialStepTestSuite.config
import etlflow.crypto.CryptoApi
import etlflow.etljobs.GenericEtlJob
import etlflow.etlsteps._
import etlflow.schema.Credential.JDBC
import etlflow.utils.JsonImplicits
import io.circe.generic.auto._
import zio.Runtime.default.unsafeRun

case class Job3DBSteps(job_properties: EtlJob4Props) extends GenericEtlJob[EtlJob4Props] with JsonImplicits with TestSuiteHelper {

  val delete_credential_script = "DELETE FROM credential WHERE name = 'etlflow'"

  val dbLog = unsafeRun((for{
    dbLog_user     <- CryptoApi.encrypt(config.db.user)
    dbLog_password <- CryptoApi.encrypt(config.db.password)
  } yield (dbLog_user, dbLog_password)).provideCustomLayer(etlflow.crypto.Implementation.live))


  val insert_credential_script = s"""
      INSERT INTO credential (name,type,value) VALUES(
      'etlflow',
      'jdbc',
      '{"url" : "${config.db.url}", "user" : "${dbLog._1}", "password" : "${dbLog._2}", "driver" : "org.postgresql.Driver" }'
      )
      """

  private val deleteCredStep = DBQueryStep(
      name  = "DeleteCredential",
      query = delete_credential_script,
      credentials = config.db
    ).process()

  private val addCredStep =  DBQueryStep(
      name  = "AddCredential",
      query = insert_credential_script,
      credentials = config.db
    ).process()

  private val creds =  GetCredentialStep[JDBC](
    name  = "GetCredential",
    credential_name = "etlflow",
  )

  private def step1(cred: JDBC) = DBReadStep[EtlJobRun](
    name  = "FetchEtlJobRun",
    query = "SELECT job_name,job_run_id,state FROM jobrun LIMIT 10",
    credentials = cred
  )

  private def processData(ip: List[EtlJobRun]): Unit = {
    logger.info("Processing Data")
    ip.foreach(jr => logger.info(jr.toString))
  }

  private def step2 = GenericETLStep(
    name               = "ProcessData",
    transform_function = processData,
  )

  val job =
    for {
      _     <- deleteCredStep
      _     <- addCredStep
      cred  <- creds.execute()
      op2   <- step1(cred).execute()
      _     <- step2.execute(op2)
    } yield ()
}
