package etlflow.task

import zio.config._
import ConfigDescriptor._
import zio.ftp.SFtp
import zio.{RIO, ZIO}

case class FTPDeleteTask(name: String, path: String) extends EtlTask[SFtp, Unit] {

  override protected def process: RIO[SFtp, Unit] = for {
    _ <- ZIO.logInfo("#" * 50)
    _ <- ZIO.logInfo(s"Deleting $path")
    _ <- SFtp.rmdir(path)
    _ <- ZIO.logInfo(s"Deleted $path")
    _ <- ZIO.logInfo("#" * 50)
  } yield ()
}

object FTPDeleteTask {
  val config: ConfigDescriptor[FTPDeleteTask] =
    string("name")
      .zip(string("path"))
      .to[FTPDeleteTask]
}
