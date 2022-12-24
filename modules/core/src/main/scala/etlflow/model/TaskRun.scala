package etlflow.model

import java.time.ZonedDateTime

case class TaskRun(
    id: String,
    jobRunId: String,
    name: String,
    taskType: String,
    props: String,
    status: String,
    createdAt: ZonedDateTime,
    modifiedAt: ZonedDateTime
)
