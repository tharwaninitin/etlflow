package etlflow.crypto

import zio.{Task, ZIO}

object CryptoApi {

  trait Service {
    def encrypt(text: String): Task[String]
    def decrypt(text: String): Task[String]
    def oneWayEncrypt(text: String, salt: Option[Int] = None): Task[String]
  }

  def encrypt(str: String): ZIO[CryptoEnv, Throwable, String] =
    ZIO.accessM(_.get.encrypt(str))
  def decrypt(str: String): ZIO[CryptoEnv, Throwable, String] =
    ZIO.accessM(_.get.decrypt(str))
  def oneWayEncrypt(text: String, salt: Option[Int] = None): ZIO[CryptoEnv, Throwable, String] =
    ZIO.accessM(_.get.oneWayEncrypt(text,salt))
}
