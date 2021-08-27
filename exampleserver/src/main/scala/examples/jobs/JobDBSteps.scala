package examples.jobs

import etlflow.etljobs.GenericEtlJob
import etlflow.etlsteps._
import etlflow.schema.Credential.JDBC
import examples.schema.MyEtlJobProps.EtlJob1Props
import examples.schema.MyEtlJobSchema.EtlJobRun

case class JobDBSteps(job_properties: EtlJob1Props) extends GenericEtlJob[EtlJob1Props] {

  val cred = JDBC("jdbc:postgresql://localhost:5432/test_db","root","root","org.postgresql.Driver")

  val delete_credential_script = "DELETE FROM credential WHERE name = 'etlflow'"

  val insert_credential_script = s"""
      INSERT INTO credential (name,type,value) VALUES(
      'etlflow',
      'jdbc',
      '{"url" : "${cred.url}", "user" : "${cred.user}", "password" : "${cred.password}", "driver" : "org.postgresql.Driver" }'
      )
      """

//  private val deleteCredStep = DBQueryStep(
//      name  = "DeleteCredential",
//      query = delete_credential_script,
//      credentials = cred
//    ).process(())
//
//  private val addCredStep =  DBQueryStep(
//      name  = "AddCredential",
//      query = insert_credential_script,
//      credentials = cred
//    ).process(())
//
  private def step1(cred: JDBC): DBReadStep[EtlJobRun] = DBReadStep[EtlJobRun](
    name  = "FetchEtlJobRun",
    query = "SELECT job_name,job_run_id,state FROM jobrun LIMIT 10",
    credentials = cred
  )(rs => EtlJobRun(rs.string("job_name"), rs.string("job_run_id"), rs.string("state")))

  private def processData(ip: String): Unit = {
    logger.info("Processing Data")
//    ip.foreach(jr => logger.info(jr.toString))
  }

  private def step2: GenericETLStep[String, Unit] = GenericETLStep(
    name               = "ProcessData",
    transform_function = processData,
  )

  val job =
    for {
//      _     <- deleteCredStep
//      _     <- addCredStep
      op2   <- step1(cred).execute(())
      _     <- step2.execute("SSSS")
    } yield ()
}
