package etlflow.coretests

trait TestSuiteHelper {
  val jsonLayer = etlflow.json.Implementation.live
  val loggerLayer = etlflow.log.Implementation.live
  val consoleLayer = etlflow.log.ConsoleImplementation.live
  val dbNoLoggerLayer = etlflow.log.DBImplementation.noLog
  val slackNoLoggerLayer = etlflow.log.SlackImplementation.nolog
  val fullCoreLayer = dbNoLoggerLayer ++ jsonLayer ++ loggerLayer ++ slackNoLoggerLayer ++ consoleLayer
  val canonical_path = new java.io.File(".").getCanonicalPath
}
