package etlflow.utils

import com.github.t3hnar.bcrypt._
import etlflow.json.{JsonApi, JsonEnv}
import etlflow.schema.Credential.{AWS, JDBC}
import zio.RIO
import etlflow.utils.CredentialImplicits._

import java.security.InvalidKeyException
import java.util.Base64
import javax.crypto.Cipher
import javax.crypto.spec.{IvParameterSpec, SecretKeySpec}
import scala.reflect.runtime.universe.{TypeTag, typeOf}

//https://docs.oracle.com/javase/7/docs/api/javax/crypto/Cipher.html
private[etlflow] object EncryptionAPI extends ApplicationLogger  with Configuration {

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
  def decrypt(text:String): String={
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

  def getDecryptValues[T : TypeTag](result: String): RIO[JsonEnv,String] = {
    typeOf[T] match {
      case t if t =:= typeOf[JDBC] =>{
        for {
          jdbc_obj <- JsonApi.convertToObject[JDBC](result)
          json    <- JsonApi.convertToString(JDBC(jdbc_obj.url, EncryptionAPI.decrypt(jdbc_obj.user), EncryptionAPI.decrypt(jdbc_obj.password), jdbc_obj.driver), List.empty)
        } yield json
      }
      case t if t =:= typeOf[AWS] =>{
        for {
          aws_obj <- JsonApi.convertToObject[AWS](result)
          json    <- JsonApi.convertToString(AWS(EncryptionAPI.decrypt(aws_obj.access_key),EncryptionAPI.decrypt(aws_obj.secret_key)),List.empty)
        } yield json
      }
    }
  }

  def encryptKey(key:String) =  {
    val salt = BCrypt.gensalt()
    key.bcryptBounded(salt)
  }
}