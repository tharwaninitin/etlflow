package etlflow.utils

import etlflow.log.ApplicationLogger
import etlflow.schema.Credential.{AWS, JDBC}
import io.circe.generic.semiauto.deriveDecoder

import java.security.InvalidKeyException
import java.util.Base64
import javax.crypto.Cipher
import javax.crypto.spec.{IvParameterSpec, SecretKeySpec}
import scala.reflect.runtime.universe.{TypeTag, typeOf}

//https://docs.oracle.com/javase/7/docs/api/javax/crypto/Cipher.html
object Encryption extends ApplicationLogger  with Configuration{

  implicit val AwsDecoder = deriveDecoder[AWS]
  implicit val JdbcDecoder = deriveDecoder[JDBC]

  final val secretKey = config.webserver.map(_.secretKey.getOrElse("enIntVecTest2020")).getOrElse("enIntVecTest2020")
  val iv = new IvParameterSpec(secretKey.getBytes("UTF-8"))
  val skeySpec = new SecretKeySpec(secretKey.getBytes("UTF-8"), "AES")
  val cipher = Cipher.getInstance("AES/CBC/PKCS5PADDING")

  //encrypt the provided key
  def encrypt(text: String): String = {
    try {
      cipher.init(Cipher.ENCRYPT_MODE, skeySpec, iv)
      val encrypted = cipher.doFinal(text.getBytes())
      Base64.getEncoder().encodeToString(encrypted)
    } catch {
      case ex : InvalidKeyException  =>
        logger.error(s"Provided key is Invalid , ${ex.getMessage}")
        throw ex
    }
  }

  //decrypt the provided key
  def decrypt(text:String) :String={
    try {
      cipher.init(Cipher.DECRYPT_MODE, skeySpec, iv)
      val decrypted = cipher.doFinal(Base64.getDecoder.decode(text))
      new String(decrypted)
    } catch {
      case ex : InvalidKeyException  =>
        logger.error(s"Provided key is Invalid , ${ex.getMessage}")
        throw ex
    }
  }

  def getDecreptValues[T : TypeTag](result: String): String = {
    typeOf[T] match {
      case t if t =:= typeOf[JDBC] =>{
        val aws_obj = JsonCirce.convertToObject[JDBC](result)
        JsonJackson.convertToJsonByRemovingKeys(JDBC(aws_obj.url, Encryption.decrypt(aws_obj.user),Encryption.decrypt(aws_obj.password),aws_obj.driver),List.empty)
      }
      case t if t =:= typeOf[AWS] =>{
        val aws_obj = JsonCirce.convertToObject[AWS](result)
        JsonJackson.convertToJsonByRemovingKeys(AWS(Encryption.decrypt(aws_obj.access_key),Encryption.decrypt(aws_obj.secret_key)),List.empty)
      }
    }
  }
}