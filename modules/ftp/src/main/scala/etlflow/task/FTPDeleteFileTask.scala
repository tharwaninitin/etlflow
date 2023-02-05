package etlflow.task

import zio.config._
import ConfigDescriptor._
import zio.ftp.SFtp
import zio.{RIO, ZIO}

case class FTPDeleteFileTask(name: String, filePath: String) extends EtlTask[SFtp, Unit] {

  override protected def process: RIO[SFtp, Unit] = for {
    _ <- ZIO.logInfo("#" * 50)
    _ <- ZIO.logInfo(s"Deleting file - $filePath")
    _ <- SFtp.rm(filePath)
    _ <- ZIO.logInfo(s"Deleted file - $filePath")
    _ <- ZIO.logInfo("#" * 50)
  } yield ()
}

object FTPDeleteFileTask {
  val config: ConfigDescriptor[FTPDeleteFileTask] =
    string("name")
      .zip(string("filePath"))
      .to[FTPDeleteFileTask]
}
