package etlflow.crypto

import etlflow.json.JsonEnv
import zio.{RIO, Task, ZIO}
import scala.reflect.runtime.universe.{typeOf, TypeTag}

object CryptoApi {

  trait Service {
    def encrypt(text: String): Task[String]
    def decrypt(text: String): Task[String]
    def decryptCredential[T: TypeTag](text: String): RIO[CryptoEnv with JsonEnv,String]
    def encryptCredential(`type`: String, value: String): RIO[CryptoEnv with JsonEnv,String]
    def oneWayEncrypt(text: String, salt: Option[Int] = None): Task[String]
  }

  def encrypt(str: String): ZIO[CryptoEnv, Throwable, String] =
    ZIO.accessM(_.get.encrypt(str))
  def decrypt(str: String): ZIO[CryptoEnv, Throwable, String] =
    ZIO.accessM(_.get.decrypt(str))
  def decryptCredential[T: TypeTag](str: String): RIO[CryptoEnv with JsonEnv,String] =
    ZIO.accessM[CryptoEnv with JsonEnv](_.get.decryptCredential[T](str))
  def encryptCredential(`type`: String, value: String): RIO[CryptoEnv with JsonEnv,String] =
    ZIO.accessM[CryptoEnv with JsonEnv](_.get.encryptCredential(`type`, value))
  def oneWayEncrypt(text: String, salt: Option[Int] = None): ZIO[CryptoEnv, Throwable, String] =
    ZIO.accessM(_.get.oneWayEncrypt(text,salt))
}
