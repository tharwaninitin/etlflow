package etlflow.coretests

trait TestSuiteHelper {
  val loggerLayer = etlflow.log.Implementation.live
  val consoleLayer = etlflow.log.ConsoleImplementation.live
  val dbNoLoggerLayer = etlflow.log.DBNoLogImplementation()
  val slackNoLoggerLayer = etlflow.log.SlackImplementation.nolog
  val fullCoreLayer = dbNoLoggerLayer ++ loggerLayer ++ slackNoLoggerLayer ++ consoleLayer
  val canonical_path = new java.io.File(".").getCanonicalPath
}
