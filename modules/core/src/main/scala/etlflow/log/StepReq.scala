package etlflow.log

import etlflow.schema.LoggingLevel

case class StepReq(job_run_id: String, slack: Option[SlackLogger] = None, job_notification_level: LoggingLevel = LoggingLevel.INFO)
