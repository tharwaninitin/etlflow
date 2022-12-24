package etlflow.model

import java.time.ZonedDateTime

case class JobRun(
    id: String,
    name: String,
    args: String,
    props: String,
    status: String,
    createdAt: ZonedDateTime,
    modifiedAt: ZonedDateTime
)
