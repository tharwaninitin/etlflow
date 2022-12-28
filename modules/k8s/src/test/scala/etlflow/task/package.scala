package etlflow

import java.util.UUID

package object task {
  lazy val id: String       = UUID.randomUUID.toString.take(8)
  val jobName: String       = s"kube-job-task-example-$id"
  val containerName: String = s"kube-container-task-example-$id"
}
