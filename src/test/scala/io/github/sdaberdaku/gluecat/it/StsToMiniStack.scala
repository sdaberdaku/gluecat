package io.github.sdaberdaku.gluecat.it

import java.net.URI

import software.amazon.awssdk.core.interceptor.{Context, ExecutionAttributes, ExecutionInterceptor, SdkExecutionAttribute}

/** Sends every STS call in the test JVM to MiniStack. SDK 2.24.6 ignores `AWS_ENDPOINT_URL`, and neither the
  * assume-role client the library builds nor the SDK's web identity provider takes an endpoint, so without this they
  * would call AWS. Registered for all SDK clients through the test resource
  * `software/amazon/awssdk/global/handlers/execution.interceptors`. */
class StsToMiniStack extends ExecutionInterceptor {
  override def beforeExecution(context: Context.BeforeExecution, attributes: ExecutionAttributes): Unit =
    if (attributes.getAttribute(SdkExecutionAttribute.SERVICE_NAME) == "Sts") {
      attributes.putAttribute(SdkExecutionAttribute.ENDPOINT_OVERRIDDEN, java.lang.Boolean.TRUE)
      attributes.putAttribute(SdkExecutionAttribute.CLIENT_ENDPOINT, URI.create(MiniStack.Endpoint))
    }
}
