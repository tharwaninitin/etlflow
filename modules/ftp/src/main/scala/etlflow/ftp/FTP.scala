package etlflow.ftp

import zio.ftp.SecureFtp.Client
import zio.ftp._
import zio.stream.ZStream
import zio.{Scope, TaskLayer, ZIO, ZLayer}
import java.io.IOException
import java.nio.file.Paths

object FTP {
  val noop: TaskLayer[SFtp] = ZLayer.succeed {
    new FtpAccessors[Client] {
      override def execute[T](f: Client => T): ZIO[Any, IOException, T] =
        ZIO.fail(new IOException(error))

      override def stat(path: String): ZIO[Any, IOException, Option[FtpResource]] =
        ZIO.fail(new IOException(error))

      override def readFile(path: String, chunkSize: Int): ZStream[Any, IOException, Byte] =
        ZStream.fail(new IOException(error))

      override def rm(path: String): ZIO[Any, IOException, Unit] =
        ZIO.fail(new IOException(error))

      override def rmdir(path: String): ZIO[Any, IOException, Unit] =
        ZIO.fail(new IOException(error))

      override def mkdir(path: String): ZIO[Any, IOException, Unit] =
        ZIO.fail(new IOException(error))

      override def ls(path: String): ZStream[Any, IOException, FtpResource] =
        ZStream.fail(new IOException(error))

      override def lsDescendant(path: String): ZStream[Any, IOException, FtpResource] =
        ZStream.fail(new IOException(error))

      override def upload[R](path: String, source: ZStream[R, Throwable, Byte]): ZIO[R with Scope, IOException, Unit] =
        ZIO.fail(new IOException(error))
    }
  }
  private val error = "FTP Actions Detected in your ETL Flow, but no Connection was provided!"

  @SuppressWarnings(Array("org.wartremover.warts.Throw"))
  def live(
      host: String,
      port: Int = 22,
      username: String,
      password: Option[String],
      privateKeyFilePath: Option[String]
  ): ZLayer[Scope, ConnectionError, SFtp] = secure {
    privateKeyFilePath match {
      case Some(path) =>
        // noinspection ScalaStyle
        if (password.isDefined) println("Password is provided, will be ignored in presence of private key file path")
        SecureFtpSettings(host, port, FtpCredentials(username, ""), KeyFileSftpIdentity(Paths.get(path)))
      case None =>
        val pass = password.getOrElse(throw new Exception("Neither Password, nor private key file path was provided!"))
        SecureFtpSettings(host, port, FtpCredentials(username, pass))
    }
  }
}
