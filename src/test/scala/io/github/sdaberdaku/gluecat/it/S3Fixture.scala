package io.github.sdaberdaku.gluecat.it

import java.net.URI

import scala.collection.JavaConverters._

import software.amazon.awssdk.auth.credentials.{AwsBasicCredentials, StaticCredentialsProvider}
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.{CreateBucketRequest, ListObjectsV2Request}

/** MiniStack S3 as one account's root key sees it (buckets are per account). */
object S3Fixture {
  def client(account: String): S3Client = S3Client.builder()
    .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(account, "test")))
    .endpointOverride(URI.create(MiniStack.Endpoint))
    .forcePathStyle(true)
    .region(Region.EU_CENTRAL_1)
    .build()

  def createBucket(account: String, name: String): Unit = withClient(account)(
    _.createBucket(CreateBucketRequest.builder().bucket(name).build()))

  def listKeys(account: String, bucket: String, prefix: String): Seq[String] = withClient(account)(
    _.listObjectsV2Paginator(ListObjectsV2Request.builder().bucket(bucket).prefix(prefix).build())
      .contents().asScala.map(_.key()).toList)

  private def withClient[T](account: String)(f: S3Client => T): T = {
    val s3 = client(account)
    try f(s3) finally s3.close()
  }
}
