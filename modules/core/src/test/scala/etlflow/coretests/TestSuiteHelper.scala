package etlflow.coretests

import etlflow.utils.{ApplicationLogger, Configuration}

trait TestSuiteHelper extends ApplicationLogger {
  val config = zio.Runtime.default.unsafeRun(Configuration.config)
  val skey = config.webserver.flatMap(_.secretKey)
  val cryptoLayer = etlflow.crypto.Implementation.live(skey)
  val jsonLayer  = etlflow.json.Implementation.live
  val loggerLayer = etlflow.log.Implementation.live
  val dbLayer = etlflow.db.liveDB(config.db)
  val slackNoLoggerLayer = etlflow.log.SlackApi.nolog
  val fullLayer = dbLayer ++ jsonLayer ++ cryptoLayer ++ loggerLayer ++ slackNoLoggerLayer
  val canonical_path = new java.io.File(".").getCanonicalPath
}
