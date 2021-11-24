package etlflow.coretests

import etlflow.DbSuiteHelper
import etlflow.utils.ApplicationLogger

trait TestSuiteHelper extends ApplicationLogger with DbSuiteHelper {
  val cryptoLayer = etlflow.crypto.Implementation.live(None)
  val jsonLayer = etlflow.json.Implementation.live
  val loggerLayer = etlflow.log.Implementation.live
  val dbLayer = etlflow.db.liveDB(credentials)
  val slackNoLoggerLayer = etlflow.log.SlackImplementation.nolog
  val consoleLayer = etlflow.log.ConsoleImplementation.live
  val fullLayer = dbLayer ++ jsonLayer ++ cryptoLayer ++ loggerLayer ++ slackNoLoggerLayer ++ consoleLayer
  val canonical_path = new java.io.File(".").getCanonicalPath
}
