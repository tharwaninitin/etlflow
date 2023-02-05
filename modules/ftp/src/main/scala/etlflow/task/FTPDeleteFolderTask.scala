package etlflow.task

import zio.config._
import ConfigDescriptor._
import zio.ftp.SFtp
import zio.{RIO, ZIO}

case class FTPDeleteFolderTask(name: String, folderPath: String) extends EtlTask[SFtp, Unit] {

  override protected def process: RIO[SFtp, Unit] = for {
    _ <- ZIO.logInfo("#" * 50)
    _ <- ZIO.logInfo(s"Deleting folder - $folderPath")
    _ <- SFtp.rmdir(folderPath)
    _ <- ZIO.logInfo(s"Deleted folder - $folderPath")
    _ <- ZIO.logInfo("#" * 50)
  } yield ()
}

object FTPDeleteFolderTask {
  val config: ConfigDescriptor[FTPDeleteFolderTask] =
    string("name")
      .zip(string("folderPath"))
      .to[FTPDeleteFolderTask]
}
