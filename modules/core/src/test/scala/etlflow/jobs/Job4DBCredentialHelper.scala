package etlflow.jobs

import etlflow.etlsteps.DBQueryStep
import etlflow.utils.Config
import zio.Runtime.default.unsafeRun

abstract class Job4DBCredentialHelper(config: Config) {

  val delete_credential_script = "DELETE FROM credentials WHERE name = 'etlflow'"

  val insert_credential_script = s"""
      INSERT INTO credentials VALUES(
      'etlflow',
      'jdbc',
      '{"url" : "${config.dbLog.url}", "user" : "${config.dbLog.user}", "password" : "${config.dbLog.password}", "driver" : "org.postgresql.Driver" }'
      )
      """

  unsafeRun(
    DBQueryStep(
      name  = "DeleteCredential",
      query = delete_credential_script,
      credentials = config.dbLog
    ).process()
  )

  unsafeRun(
    DBQueryStep(
      name  = "AddCredential",
      query = insert_credential_script,
      credentials = config.dbLog
    ).process()
  )
}
