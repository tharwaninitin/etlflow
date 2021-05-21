package etlflow.coretests.jobs

import etlflow.coretests.Schema.{EtlJob4Props, EtlJobRun}
import etlflow.etljobs.GenericEtlJob
import etlflow.etlsteps._
import etlflow.Credential.JDBC
import etlflow.utils.Encryption
case class Job4DBSteps(job_properties: EtlJob4Props) extends GenericEtlJob[EtlJob4Props] {

  val delete_credential_script = "DELETE FROM credential WHERE name = 'etlflow'"

  val dbLog_user = Encryption.encrypt(config.dbLog.user)
  val dbLog_password = Encryption.encrypt(config.dbLog.password)

  val insert_credential_script = s"""
      INSERT INTO credential (name,type,value) VALUES(
      'etlflow',
      'jdbc',
      '{"url" : "${config.dbLog.url}", "user" : "${dbLog_user}", "password" : "${dbLog_password}", "driver" : "org.postgresql.Driver" }'
      )
      """

  private val deleteCredStep = DBQueryStep(
      name  = "DeleteCredential",
      query = delete_credential_script,
      credentials = config.dbLog
    ).process()

  private val addCredStep =  DBQueryStep(
      name  = "AddCredential",
      query = insert_credential_script,
      credentials = config.dbLog
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
