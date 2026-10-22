package io.github.sdaberdaku.gluecat

import org.junit.runner.RunWith
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class SparkVersionCheckSuite extends AnyFunSuite {
  test("supported versions pass") {
    Seq("3.5.3", "3.5.9", "4.0.0", "4.2.0", "4.1.3-SNAPSHOT").foreach(SparkVersionCheck.requireSupported)
  }

  test("versions below 3.5.3 are rejected") {
    Seq("3.5.2", "3.5.0", "3.4.4", "2.4.8").foreach { v =>
      val e = intercept[IllegalArgumentException](SparkVersionCheck.requireSupported(v))
      assert(e.getMessage.contains("3.5.3"))
    }
  }
}
