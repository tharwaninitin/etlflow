package etlflow

import doobie.Transactor
import doobie.util.transactor.Transactor.Aux
import zio.Task
import zio.interop.catz._
import zio.interop.catz.implicits._

trait DoobieHelper {
  def transactor(url: String, user: String, pwd: String): Aux[Task, Unit]
  = Transactor.fromDriverManager[Task]("org.postgresql.Driver", url, user, pwd)
}
