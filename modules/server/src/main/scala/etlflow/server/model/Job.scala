package etlflow.server.model

case class Job(
    name: String,
    props: Map[String, String],
    schedule: String,
    nextSchedule: String,
    schduleRemainingTime: String,
    failed: Long,
    success: Long,
    is_active: Boolean,
    last_run_time: Long,
    last_run_description: String
)
