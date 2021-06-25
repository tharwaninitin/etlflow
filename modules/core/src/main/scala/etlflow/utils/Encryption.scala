package etlflow.utils

import etlflow.json.{Implementation, JsonImplicits, JsonService}
import etlflow.log.ApplicationLogger
import etlflow.schema.Credential.{AWS, JDBC}
import io.circe.Json
import zio.Task

import java.security.InvalidKeyException
import java.util.Base64
import javax.crypto.Cipher
import javax.crypto.spec.{IvParameterSpec, SecretKeySpec}
import scala.reflect.runtime.universe.{TypeTag, typeOf}
import etlflow.json.CredentialImplicits._
//https://docs.oracle.com/javase/7/docs/api/javax/crypto/Cipher.html
private[etlflow] object Encryption extends ApplicationLogger  with Configuration {

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

  def getDecreptValues[T : TypeTag](result: String): Task[Json] = {
    typeOf[T] match {
      case t if t =:= typeOf[JDBC] =>{
        for {
          jdbc_obj <- JsonService.convertToObject[JDBC](result)
          json    <- JsonService.convertToJsonByRemovingKeys(JDBC(jdbc_obj.url, Encryption.decrypt(jdbc_obj.user), Encryption.decrypt(jdbc_obj.password), jdbc_obj.driver), List.empty)
        } yield json
      }.provideLayer(Implementation.live)
      case t if t =:= typeOf[AWS] =>{
        for {
          aws_obj <- JsonService.convertToObject[AWS](result)
          json    <- JsonService.convertToJsonByRemovingKeys(AWS(Encryption.decrypt(aws_obj.access_key),Encryption.decrypt(aws_obj.secret_key)),List.empty)
        } yield json
      }.provideLayer(Implementation.live)
    }
  }
}