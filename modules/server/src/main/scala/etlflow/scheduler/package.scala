package etlflow

import com.cronutils.model.Cron

package object scheduler {
  class CronJob(val name: String, val schedule: Option[Cron])
}
