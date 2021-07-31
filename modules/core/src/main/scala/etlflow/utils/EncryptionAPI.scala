package etlflow.utils

import com.github.t3hnar.bcrypt._
import etlflow.json.{JsonApi, JsonEnv}
import etlflow.schema.Credential.{AWS, JDBC}
import etlflow.utils.CredentialImplicits._
import zio.RIO
import java.util.Base64
import javax.crypto.Cipher
import javax.crypto.spec.{IvParameterSpec, SecretKeySpec}
import scala.reflect.runtime.universe.{TypeTag, typeOf}

//https://docs.oracle.com/javase/7/docs/api/javax/crypto/Cipher.html
private[etlflow] case class EncryptionAPI(key: Option[String] = None) {

  final val secretKey = key.getOrElse("enIntVecTest2020")
  final val iv = new IvParameterSpec(secretKey.getBytes("UTF-8"))
  final val skeySpec = new SecretKeySpec(secretKey.getBytes("UTF-8"), "AES")
  final val cipher = Cipher.getInstance("AES/CBC/PKCS5PADDING")

  // encrypt the provided string
  def encrypt(text: String): String = {
    cipher.init(Cipher.ENCRYPT_MODE, skeySpec, iv)
    val encrypted = cipher.doFinal(text.getBytes())
    Base64.getEncoder.encodeToString(encrypted)
  }

  // decrypt the provided string
  def decrypt(text: String): String={
    cipher.init(Cipher.DECRYPT_MODE, skeySpec, iv)
    val decrypted = cipher.doFinal(Base64.getDecoder.decode(text))
    new String(decrypted)
  }

  // decrypt the provided credential string
  def decryptCredential[T : TypeTag](text: String): RIO[JsonEnv,String] = {
    typeOf[T] match {
      case t if t =:= typeOf[JDBC] =>
        for {
          jdbc <- JsonApi.convertToObject[JDBC](text)
          json <- JsonApi.convertToString(JDBC(jdbc.url, decrypt(jdbc.user), decrypt(jdbc.password), jdbc.driver), List.empty)
        } yield json
      case t if t =:= typeOf[AWS] =>
        for {
          aws  <- JsonApi.convertToObject[AWS](text)
          json <- JsonApi.convertToString(AWS(decrypt(aws.access_key), decrypt(aws.secret_key)), List.empty)
        } yield json
    }
  }

  // encrypt the provided credential string
  def encryptCredential(`type`: String, value: String): RIO[JsonEnv, String] = {
    `type` match {
      case "jdbc" =>
        for {
          jdbc <- JsonApi.convertToObject[JDBC](value)
          json <- JsonApi.convertToString(JDBC(jdbc.url, encrypt(jdbc.user), encrypt(jdbc.password), jdbc.driver), List.empty)
        } yield json
      case "aws" =>
        for {
          aws  <- JsonApi.convertToObject[AWS](value)
          json <- JsonApi.convertToString(AWS(encrypt(aws.access_key), encrypt(aws.secret_key)), List.empty)
        } yield json
    }
  }

  // One way encrypt the provided string
  def oneWayEncrypt(text: String): String =  {
    val salt = BCrypt.gensalt()
    text.bcryptBounded(salt)
  }
}