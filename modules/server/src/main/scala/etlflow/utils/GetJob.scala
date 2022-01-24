package etlflow.utils

import com.cronutils.model.time.ExecutionTime
import cron4zio._
import etlflow.server.model.{JobDBAll, Job}
import etlflow.utils.DateTimeApi.{getLocalDateTimeFromTimestamp, getTimeDifferenceAsString}
import org.ocpsoft.prettytime.PrettyTime
import java.time.LocalDateTime
import java.util.TimeZone

object GetJob {
  def apply(schedule: String, jdb: JobDBAll, lastRunTime: String, props: Map[String, String]): Job = {
    val pt     = new PrettyTime()
    val cron   = parse(schedule).toOption
    val zoneId = TimeZone.getDefault.toZoneId
    if (cron.isDefined) {
      val startTimeMillis: Long = LocalDateTime.now().atZone(zoneId).toInstant.toEpochMilli
      val endTimeMillis: Option[Long] = Some(
        ExecutionTime
          .forCron(cron.get)
          .nextExecution(LocalDateTime.now().atZone(zoneId))
          .get
          .toInstant
          .toEpochMilli
      )
      val remTime1         = endTimeMillis.map(ts => getTimeDifferenceAsString(startTimeMillis, ts)).getOrElse("")
      val remTime2         = endTimeMillis.map(ts => pt.format(getLocalDateTimeFromTimestamp(ts))).getOrElse("")
      val nextScheduleTime = schedule
      Job(
        jdb.job_name,
        props,
        schedule,
        nextScheduleTime,
        s"$remTime2 ($remTime1)",
        jdb.failed,
        jdb.success,
        jdb.is_active,
        jdb.last_run_time.getOrElse(0),
        s"$lastRunTime"
      )
    } else {
      Job(
        jdb.job_name,
        props,
        "",
        "",
        "",
        jdb.failed,
        jdb.success,
        jdb.is_active,
        jdb.last_run_time.getOrElse(0),
        s"$lastRunTime"
      )
    }
  }
}
