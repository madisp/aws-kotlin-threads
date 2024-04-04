plugins {
  alias(libs.plugins.kotlin.jvm)
}

dependencies {
  testImplementation(libs.kotlinx.coroutines.core)
  testImplementation(libs.aws.kotlin.sqs)
  testImplementation(libs.okio)
  testImplementation(libs.okhttp)
  testImplementation(libs.kotlinx.coroutines.test)
  testImplementation(libs.junit)
  testImplementation(libs.testcontainers.localstack)
}
