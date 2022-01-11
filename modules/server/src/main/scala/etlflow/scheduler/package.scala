package etlflow

import com.cronutils.model.time.ExecutionTime
import cron4zio.parseCron

package object scheduler {
  class Cron(val name: String, val cron: Option[ExecutionTime])
  object Cron {
    def apply(cron: String) = new Cron(cron, parseCron(cron).toOption)
  }
  case class CronJob(job_name: String, schedule: Cron)
}
