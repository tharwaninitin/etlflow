package etlflow.crypto

import etlflow.json.{JsonApi, JsonEnv}
import etlflow.schema.Credential.{AWS, JDBC}
import org.mindrot.jbcrypt.BCrypt
import zio.{RIO, Tag, Task, ULayer, ZLayer}
import java.util.Base64
import javax.crypto.Cipher
import javax.crypto.spec.{IvParameterSpec, SecretKeySpec}
import io.circe.generic.auto._

object Implementation {

  def live(key: Option[String]): ULayer[CryptoEnv] = ZLayer.succeed(
    new CryptoApi.Service {

      final val secretKey = key.getOrElse(etlflow.utils.Defaults.secretkey)
      final val iv = new IvParameterSpec(secretKey.getBytes("UTF-8"))
      final val skeySpec = new SecretKeySpec(secretKey.getBytes("UTF-8"), "AES")
      final val cipher = Cipher.getInstance("AES/CBC/PKCS5PADDING")

      override def encrypt(text: String): Task[String] = Task{
        cipher.init(Cipher.ENCRYPT_MODE, skeySpec, iv)
        val encrypted = cipher.doFinal(text.getBytes())
        Base64.getEncoder.encodeToString(encrypted)
      }

      override def decrypt(text: String): Task[String] = Task{
        cipher.init(Cipher.DECRYPT_MODE, skeySpec, iv)
        val decrypted = cipher.doFinal(Base64.getDecoder.decode(text))
        new String(decrypted)
      }

      override def decryptCredential[T: Tag](text: String): RIO[CryptoEnv with JsonEnv,String] = {
        implicitly[Tag[T]].tag match {
          case t if t =:= Tag[JDBC].tag =>
            for {
              jdbc                <- JsonApi.convertToObject[JDBC](text)
              decrypt_user        <- decrypt(jdbc.user)
              decrypt_password    <- decrypt(jdbc.password)
              json                <- JsonApi.convertToString(JDBC(jdbc.url, decrypt_user, decrypt_password, jdbc.driver), List.empty)
            } yield json
          case t if t =:= Tag[AWS].tag =>
            for {
              aws        <- JsonApi.convertToObject[AWS](text)
              decrypt_access_key <- decrypt(aws.access_key)
              decrypt_secret_key <- decrypt(aws.secret_key)
              json       <- JsonApi.convertToString(AWS(decrypt_access_key, decrypt_secret_key), List.empty)
            } yield json
        }
      }

      override def encryptCredential(`type`: String, value: String): RIO[CryptoEnv with JsonEnv,String] = {
        `type` match {
          case "jdbc" =>
            for {
              jdbc                <- JsonApi.convertToObject[JDBC](value)
              encrypt_user        <- encrypt(jdbc.user)
              encrypt_password    <- encrypt(jdbc.password)
              json <- JsonApi.convertToString(JDBC(jdbc.url, encrypt_user, encrypt_password, jdbc.driver), List.empty)
            } yield json
          case "aws" =>
            for {
              aws  <- JsonApi.convertToObject[AWS](value)
              encrypt_access_key <- encrypt(aws.access_key)
              encrypt_secret_key <- encrypt(aws.secret_key)
              json <- JsonApi.convertToString(AWS(encrypt_access_key, encrypt_secret_key), List.empty)
            } yield json
        }
      }

      override def oneWayEncrypt(text: String, salt: Option[Int] = None): Task[String] = Task{
        val log_rounds = BCrypt.gensalt(salt.getOrElse(10))
        BCrypt.hashpw(text, log_rounds)
      }
    }
  )
}

