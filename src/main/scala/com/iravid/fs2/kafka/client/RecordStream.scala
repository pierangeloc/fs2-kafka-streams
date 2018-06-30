package com.iravid.fs2.kafka.client

import cats.effect._, cats.effect.implicits._, cats.implicits._
import cats.effect.concurrent.Ref
import com.iravid.fs2.kafka.EnvT
import com.iravid.fs2.kafka.codecs.KafkaDecoder
import com.iravid.fs2.kafka.model.{ ByteRecord, ConsumerMessage, Result }
import fs2._
import org.apache.kafka.common.TopicPartition

object RecordStream {
  case class Partitioned[F[_], T](
    commitQueue: CommitQueue[F],
    records: Stream[F, (TopicPartition, Stream[F, ConsumerMessage[Result, T]])])
  case class Plain[F[_], T](commitQueue: CommitQueue[F],
                            records: Stream[F, ConsumerMessage[Result, T]])

  case class PartitionHandle[F[_]](data: async.mutable.Queue[F, Option[ByteRecord]]) {
    def records: Stream[F, ByteRecord] =
      data.dequeue.unNoneTerminate
  }

  object PartitionHandle {
    def fromTopicPartition[F[_]: Concurrent](
      tp: TopicPartition,
      bufferSize: Int): F[(TopicPartition, PartitionHandle[F])] =
      async
        .boundedQueue[F, Option[ByteRecord]](bufferSize)
        .map(queue => (tp, PartitionHandle(queue)))
  }

  def partitioned[F[_], T: KafkaDecoder](
    settings: ConsumerSettings,
    consumer: Consumer[F],
    subscription: Subscription
  )(implicit F: ConcurrentEffect[F], timer: Timer[F]): Resource[F, Partitioned[F, T]] =
    for {
      pendingRebalances <- Resource.liftF(Ref[F].of(List[Rebalance]()))
      rebalanceListener: Rebalance.Listener[F] = rebalance =>
        pendingRebalances.update(rebalance :: _)

      _ <- Resource.make(consumer.subscribe(subscription, rebalanceListener))(_ =>
            consumer.unsubscribe)

      partitionTracker <- Resource.liftF(Ref[F].of(Map.empty[TopicPartition, PartitionHandle[F]]))
      partitionsQueue <- Resource.liftF(
                          async
                            .unboundedQueue[
                              F,
                              Either[
                                Throwable,
                                Option[(TopicPartition, Stream[F, ConsumerMessage[Result, T]])]]])
      partitionsOut = partitionsQueue.dequeue.rethrow.unNoneTerminate

      commitQueue <- Resource.liftF(CommitQueue.create[F](settings.maxPendingCommits))
      commits         = commitQueue.queue.dequeue
      polls           = Stream(Poll) ++ Stream.fixedRate(settings.pollInterval).as(Poll)
      commitsAndPolls = commits.either(polls).map(_.some)

      shutdownQueue <- Resource.liftF(async.boundedQueue[F, None.type](1))
      commandStream = shutdownQueue.dequeue
        .mergeHaltL(commitsAndPolls)
        .unNoneTerminate

      _ <- Resource.make {
            commandStream
              .evalMap {
                case Left((deferred, req)) =>
                  (consumer
                    .commit(req.offsets)
                    .void
                    .attempt >>= deferred.complete).void
                case Right(Poll) =>
                  for {
                    records <- consumer
                                .poll(settings.pollTimeout, settings.wakeupTimeout)
                    rebalances <- pendingRebalances.getAndSet(Nil)
                    _ <- rebalances.reverse traverse_ {
                          case Rebalance.Assign(partitions) =>
                            for {
                              tracker <- partitionTracker.get
                              handles <- partitions.traverse(
                                          PartitionHandle
                                            .fromTopicPartition(
                                              _,
                                              settings.partitionOutputBufferSize))
                              _ <- partitionTracker.set(tracker ++ handles)
                              _ <- handles.traverse_ {
                                    case (tp, h) =>
                                      partitionsQueue.enqueue1(
                                        (tp, h.records through deserialize[F, T]).some.asRight)
                                  }
                            } yield ()
                          case Rebalance.Revoke(partitions) =>
                            for {
                              tracker <- partitionTracker.get
                              handles = partitions.flatMap(tracker.get)
                              _ <- handles.traverse_(_.data.enqueue1(none))
                              _ <- partitionTracker.set(tracker -- partitions)
                            } yield ()
                        }
                    tracker <- partitionTracker.get
                    _ <- records.traverse_ { record =>
                          tracker
                            .get(new TopicPartition(record.topic, record.partition)) match {
                            case Some(handle) =>
                              handle.data.enqueue1(record.some)
                            case None =>
                              F.raiseError[Unit](
                                new Exception("Got records for untracked partition"))
                          }
                        }
                  } yield ()
              }
              .compile
              .drain
              .start
          }(fiber => shutdownQueue.enqueue1(None) *> fiber.join)

    } yield Partitioned(commitQueue, partitionsOut)

  def plain[F[_]: ConcurrentEffect: Timer, T: KafkaDecoder](
    settings: ConsumerSettings,
    consumer: Consumer[F],
    subscription: Subscription): Resource[F, Plain[F, T]] =
    partitioned[F, T](settings, consumer, subscription).map { partitionedRecordStream =>
      Plain(
        partitionedRecordStream.commitQueue,
        partitionedRecordStream.records.map {
          case (_, stream) => stream
        }.joinUnbounded
      )
    }

  def deserialize[F[_], T: KafkaDecoder]: Pipe[F, ByteRecord, ConsumerMessage[Result, T]] =
    _.map(rec => EnvT(rec, KafkaDecoder[T].decode(rec)))
}
