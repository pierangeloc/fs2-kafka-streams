package com.iravid.fs2.kafka.client

import cats.implicits._
import cats.effect.IO
import com.iravid.fs2.kafka.codecs.{ KafkaDecoder, KafkaEncoder }
import fs2.Stream
import net.manub.embeddedkafka.{ EmbeddedKafka, EmbeddedKafkaConfig }
import org.scalacheck.{ Gen, Shrink }
import org.scalatest._
import org.scalatest.Matchers._
import org.scalatest.prop.GeneratorDrivenPropertyChecks

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

trait KafkaSettings extends EmbeddedKafka { self: Suite =>
  def kafkaConfig: EmbeddedKafkaConfig =
    EmbeddedKafkaConfig(kafkaPort = 0, zooKeeperPort = 0)

  def mkConsumerSettings(port: Int, groupId: String) = ConsumerSettings(
    Map(
      "bootstrap.servers" -> s"localhost:$port",
      "group.id"          -> groupId,
      "auto.offset.reset" -> "earliest"
    ),
    10,
    10,
    10,
    50.millis,
    50.millis,
    10.seconds
  )

  def mkProducerSettings(port: Int) = ProducerSettings(
    Map(
      "bootstrap.servers" -> s"localhost:${port}",
    ),
    5.seconds
  )

  implicit def decoder: KafkaDecoder[String] =
    KafkaDecoder.instance(rec => Right(new String(rec.value, "UTF-8")))

  implicit def encoder: KafkaEncoder[String] =
    KafkaEncoder.instance(str => (None, KafkaEncoder.Value(str.getBytes)))

  def produce(settings: ProducerSettings, topic: String, data: List[(Int, String)]) =
    Producer.create[IO](settings) use { producer =>
      data traverse {
        case (partition, msg) =>
          Producer.produce[IO, String](producer, msg, topic, partition, None)
      }
    }

  def nonEmptyStr = Gen.nonEmptyListOf(Gen.alphaLowerChar).map(_.mkString)

  implicit def noShrink[T]: Shrink[T] = Shrink.shrinkAny
}

class RecordStreamIntegrationSpec
    extends WordSpec with KafkaSettings with GeneratorDrivenPropertyChecks {
  def recordStream(settings: ConsumerSettings, topic: String) =
    for {
      consumer     <- KafkaConsumer[IO](settings)
      recordStream <- RecordStream[IO, String](settings, consumer, Subscription.Topics(List(topic)))
    } yield recordStream

  def program(consumerSettings: ConsumerSettings,
              producerSettings: ProducerSettings,
              topic: String,
              data: List[String]) =
    for {
      _ <- produce(producerSettings, topic, data.tupleLeft(0))
      results <- recordStream(consumerSettings, topic) use { recordStream =>
                  recordStream.records
                    .segmentN(data.length, true)
                    .map(_.force.toChunk)
                    .evalMap { records =>
                      val commitReq =
                        records.foldMap(record =>
                          CommitRequest(record.env.topic, record.env.partition, record.env.offset))

                      IO {
                        println(records.map(_.fa.getOrElse("Error")))
                        records.map(_.fa)
                      } <* recordStream.commitQueue.requestCommit(commitReq)
                    }
                    .flatMap(Stream.chunk(_).covary[IO])
                    .take(data.length.toLong)
                    .compile
                    .toVector
                    .timeout(10.seconds)
                }
    } yield results

  "The plain consumer" ignore {
    "work properly" in withRunningKafkaOnFoundPort(kafkaConfig) { config =>
      forAll(nonEmptyStr, nonEmptyStr, Gen.listOf(Gen.alphaStr)) {
        (groupId: String, topic: String, data: List[String]) =>
          val consumerSettings =
            mkConsumerSettings(config.kafkaPort, groupId).copy(outputBufferSize = data.length)
          val producerSettings = mkProducerSettings(config.kafkaPort)
          val results =
            program(consumerSettings, producerSettings, topic, data)
              .unsafeRunSync()

          results.collect { case Right(a) => a } should contain theSameElementsAs data
      }
    }
  }
}

class PartitionedRecordStreamIntegrationSpec
    extends WordSpec with KafkaSettings with GeneratorDrivenPropertyChecks {
  def partitionedRecordStream(settings: ConsumerSettings, topic: String) =
    for {
      consumer <- KafkaConsumer[IO](settings)
      recordStream <- PartitionedRecordStream[IO, String](
                       settings,
                       consumer,
                       Subscription.Topics(List(topic)))
    } yield recordStream

  def program(consumerSettings: ConsumerSettings,
              producerSettings: ProducerSettings,
              topic: String,
              data: List[(Int, String)]) =
    for {
      _ <- produce(producerSettings, topic, data)
      results <- partitionedRecordStream(consumerSettings, topic) use { stream =>
                  stream.records
                    .evalMap {
                      case (tp, stream) =>
                        IO {
                          println(s"Assigned partition: ${tp}")
                          stream
                            .onError {
                              case e => Stream.eval(IO(println(s"Stream failure: ${e}")))
                            }
                            .onFinalize(IO(println(s"Stream ended: ${tp}")))
                        }
                    }
                    .joinUnbounded
                    .chunks
                    .evalMap { recs =>
                      IO {
                        println(recs.map(_.fa.getOrElse("Error")))
                        recs.map(_.fa)
                      } <* stream.commitQueue.requestCommit(recs.foldMap(rec =>
                        CommitRequest(rec.env.topic, rec.env.partition, rec.env.offset)))
                    }
                    .flatMap(Stream.chunk(_).covary[IO])
                    .take(data.length.toLong)
                    .compile
                    .toVector
                    .timeout(30.seconds)
                }
    } yield results

  "The partitioned consumer" should {
    "work properly" in withRunningKafkaOnFoundPort(kafkaConfig) { implicit config =>
      val dataGen = for {
        partitions <- Gen.chooseNum(1, 8)
        data       <- Gen.listOf(Gen.zip(Gen.chooseNum(0, partitions - 1), Gen.alphaStr))
      } yield (partitions, data)

      forAll((nonEmptyStr, "topic"), (nonEmptyStr, "groupId"), (dataGen, "data")) {
        case (topic, groupId, (partitions, data)) =>
          createCustomTopic(topic, partitions = partitions)

          val consumerSettings = mkConsumerSettings(config.kafkaPort, groupId)
            .copy(partitionOutputBufferSize = data.length)
          val producerSettings = mkProducerSettings(config.kafkaPort)
          val results =
            program(consumerSettings, producerSettings, topic, data)
              .unsafeRunSync()

          results.collect { case Right(a) => a } should contain theSameElementsAs (data.map(_._2))
      }
    }
  }
}
