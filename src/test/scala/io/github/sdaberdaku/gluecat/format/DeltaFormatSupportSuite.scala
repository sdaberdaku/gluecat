package io.github.sdaberdaku.gluecat.format

import org.junit.runner.RunWith
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class DeltaFormatSupportSuite extends AnyFunSuite {
  test("s3, s3a and s3n are the same location scheme; bucket and path still matter") {
    assert(DeltaFormatSupport.sameLocation("s3://b/t", "s3a://b/t"))
    assert(DeltaFormatSupport.sameLocation("s3n://b/t/", "s3://b/t"))
    assert(!DeltaFormatSupport.sameLocation("s3://b/t", "s3://c/t"))
    assert(!DeltaFormatSupport.sameLocation("s3://b/t", "s3://b/u"))
    assert(!DeltaFormatSupport.sameLocation("s3://b/t", "file:/b/t"))
  }
}
