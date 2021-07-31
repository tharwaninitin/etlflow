package etlflow.coretests.utils

import com.github.t3hnar.bcrypt._
import etlflow.coretests.TestSuiteHelper
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should

class EncryptTestSuite extends AnyFlatSpec with should.Matchers with TestSuiteHelper {
  "Encrypt should " should "retrieve correct value" in {
    assert(enc.encrypt("admin") == "twV4rChhxs76Z+gY868NSw==")
  }

  "ASymmetric Encrypted key should " should "not be Bcrypt Bounded" in {
    val password: String = enc.oneWayEncrypt("abc")
    assert(!password.isBcryptedBounded(password))
  }
}
