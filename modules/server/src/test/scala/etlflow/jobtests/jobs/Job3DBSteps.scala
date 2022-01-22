package etlflow.jobtests.jobs

import crypto4s.Crypto
import etlflow.etljobs.EtlJob
import etlflow.etlsteps._
import etlflow.jobtests.ConfigHelper
import etlflow.jobtests.MyEtlJobProps.EtlJob4Props
import etlflow.model.Credential.JDBC
import io.circe.generic.auto._

case class Job3DBSteps(job_properties: EtlJob4Props) extends EtlJob[EtlJob4Props] with ConfigHelper {

  val delete_credential_script = "DELETE FROM credential WHERE name = 'etlflow'"

  val crypto         = Crypto(config.secretkey)
  val dbLog_user     = crypto.encrypt(config.db.get.user)
  val dbLog_password = crypto.encrypt(config.db.get.password)

  val insert_credential_script = s"""
      INSERT INTO credential (name,type,value) VALUES(
      'etlflow',
      'jdbc',
      '{"url" : "${config.db.get.url}", "user" : "$dbLog_user", "password" : "$dbLog_password", "driver" : "org.postgresql.Driver" }'
      )
      """

  private val deleteCredStep = DBQueryStep(
    name = "DeleteCredential",
    query = delete_credential_script,
    credentials = config.db.get
  ).process

  private val addCredStep = DBQueryStep(
    name = "AddCredential",
    query = insert_credential_script,
    credentials = config.db.get
  ).process

  private val creds = GetCredentialStep[JDBC](
    name = "GetCredential",
    credential_name = "etlflow"
  )
  case class EtlJobRun(job_name: String, job_run_id: String, state: String)
  private def step1(cred: JDBC): DBReadStep[EtlJobRun] = DBReadStep[EtlJobRun](
    name = "FetchEtlJobRun",
    query = "SELECT job_name,job_run_id,state FROM jobrun LIMIT 10",
    credentials = cred
  )(rs => EtlJobRun(rs.string("job_name"), rs.string("job_run_id"), rs.string("state")))

  private def processData(ip: List[EtlJobRun]): Unit = {
    logger.info("Processing Data")
    ip.foreach(jr => logger.info(jr.toString))
  }

  private def step2(ip: List[EtlJobRun]): GenericETLStep[Unit] = GenericETLStep(
    name = "ProcessData",
    function = processData(ip)
  )

  val job =
    for {
      _    <- deleteCredStep
      _    <- addCredStep
      cred <- creds.execute
      op2  <- step1(cred).execute
      _    <- step2(op2).execute
    } yield ()
}
