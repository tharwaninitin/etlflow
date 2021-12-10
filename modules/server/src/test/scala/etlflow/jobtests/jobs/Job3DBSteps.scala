package etlflow.jobtests.jobs

import etlflow.coretests.Schema.EtlJobRun
import etlflow.jobtests.MyEtlJobProps.EtlJob4Props
import etlflow.coretests.Schema
import etlflow.crypto.CryptoApi
import etlflow.etljobs.EtlJob
import etlflow.etlsteps._
import etlflow.jobtests.ConfigHelper
import etlflow.schema.Credential.JDBC
import io.circe.generic.auto._
import zio.Runtime.default.unsafeRun

case class Job3DBSteps(job_properties: EtlJob4Props) extends EtlJob[EtlJob4Props] with ConfigHelper {

  val delete_credential_script = "DELETE FROM credential WHERE name = 'etlflow'"

  val dbLog = unsafeRun((for{
    dbLog_user     <- CryptoApi.encrypt(config.db.get.user)
    dbLog_password <- CryptoApi.encrypt(config.db.get.password)
  } yield (dbLog_user, dbLog_password)).provideCustomLayer(etlflow.crypto.Implementation.live(None)))


  val insert_credential_script = s"""
      INSERT INTO credential (name,type,value) VALUES(
      'etlflow',
      'jdbc',
      '{"url" : "${config.db.get.url}", "user" : "${dbLog._1}", "password" : "${dbLog._2}", "driver" : "org.postgresql.Driver" }'
      )
      """

  private val deleteCredStep = DBQueryStep(
      name  = "DeleteCredential",
      query = delete_credential_script,
      credentials = config.db.get
    ).process(())

  private val addCredStep =  DBQueryStep(
      name  = "AddCredential",
      query = insert_credential_script,
      credentials = config.db.get
    ).process(())

  private val creds =  GetCredentialStep[JDBC](
    name  = "GetCredential",
    credential_name = "etlflow",
  )

  private def step1(cred: JDBC): DBReadStep[Schema.EtlJobRun] = DBReadStep[Schema.EtlJobRun](
    name  = "FetchEtlJobRun",
    query = "SELECT job_name,job_run_id,state FROM jobrun LIMIT 10",
    credentials = cred
  )(rs => EtlJobRun(rs.string("job_name"), rs.string("job_run_id"), rs.string("state")))

  private def processData(ip: List[Schema.EtlJobRun]): Unit = {
    logger.info("Processing Data")
    ip.foreach(jr => logger.info(jr.toString))
  }

  private def step2: GenericETLStep[List[Schema.EtlJobRun], Unit] = GenericETLStep(
    name               = "ProcessData",
    transform_function = processData,
  )

  val job =
    for {
      _     <- deleteCredStep
      _     <- addCredStep
      cred  <- creds.execute(())
      op2   <- step1(cred).execute(())
      _     <- step2.execute(op2)
    } yield ()
}
