import aws.sdk.kotlin.runtime.auth.credentials.StaticCredentialsProvider
import aws.sdk.kotlin.services.sqs.SqsClient
import aws.sdk.kotlin.services.sqs.model.CreateQueueRequest
import aws.sdk.kotlin.services.sqs.model.DeleteMessageRequest
import aws.sdk.kotlin.services.sqs.model.ReceiveMessageRequest
import aws.sdk.kotlin.services.sqs.model.SendMessageBatchRequest
import aws.sdk.kotlin.services.sqs.model.SendMessageBatchRequestEntry
import aws.smithy.kotlin.runtime.net.url.Url
import aws.smithy.kotlin.runtime.time.Instant
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.async
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.merge
import kotlinx.coroutines.flow.take
import kotlinx.coroutines.flow.toCollection
import kotlinx.coroutines.test.runTest
import org.junit.Rule
import org.junit.Test
import org.testcontainers.containers.localstack.LocalStackContainer
import org.testcontainers.utility.DockerImageName
import java.util.UUID
import kotlin.time.Duration.Companion.minutes
import kotlin.time.measureTime

const val MESSAGES = 1000
const val CONSUMERS = 100

class SqsKotlinLoadTest {
  @get:Rule
  val localstack = LocalStackContainer(DockerImageName.parse("localstack/localstack:3.1.0"))
    .withServices(LocalStackContainer.Service.SQS)

  @Test
  fun test() = runTest(timeout = 10.minutes) {
    GlobalScope.async {
      val sqsClient = SqsClient {
        region = "eu-west-1"
        endpointUrl = Url.parse(localstack.getEndpointOverride(LocalStackContainer.Service.SQS).toString())
        credentialsProvider = StaticCredentialsProvider {
          accessKeyId = "local"
          secretAccessKey = "hunter2"
        }
      }

      val q = sqsClient.createQueue(CreateQueueRequest {
        queueName = "test-${UUID.randomUUID()}"
      })

      (0 until MESSAGES).chunked(10).map { chunk ->
        sqsClient.sendMessageBatch(SendMessageBatchRequest {
          queueUrl = q.queueUrl
          entries = chunk.map {
            SendMessageBatchRequestEntry {
              id = it.toString()
              messageBody = "test_$it"
            }
          }
        })
      }

      val consumers = (0 until CONSUMERS).map {
        consume(it.toString(), sqsClient, q.queueUrl!!)
      }

      val received = mutableListOf<Recv>()
      val time = measureTime {
        merge(*consumers.toTypedArray()).take(MESSAGES).toCollection(received)
      }

      println("Done in $time")
      received.groupBy { it.handler }.map { (k, v) ->
        "${k.padStart(4, '0')}: ${v.size}"
      }.sorted().forEach { println(it) }

      sqsClient.close()
    }.await()
  }

  suspend fun consume(id: String, client: SqsClient, queueUrl: String) = flow {
    while (true) {
      val messages = client.receiveMessage(ReceiveMessageRequest {
        this.queueUrl = queueUrl
        maxNumberOfMessages = 1
      }).messages

      messages?.forEach {
        emit(Recv(it.body!!, id, Instant.now()))
        client.deleteMessage(DeleteMessageRequest {
          this.queueUrl = queueUrl
          this.receiptHandle = it.receiptHandle!!
        })
      }
    }
  }
}

data class Recv(
  val body: String,
  val handler: String,
  val ts: Instant,
)
