package com.iravid.fs2.kafka.client

import cats.effect.concurrent.Deferred
import cats.{ Apply, Functor }
import cats.effect._, cats.effect.implicits._, cats.implicits._
import cats.effect.concurrent.Ref
import com.iravid.fs2.kafka.EnvT
import com.iravid.fs2.kafka.codecs.KafkaDecoder
import com.iravid.fs2.kafka.model.{ ByteRecord, ConsumerMessage, Result }
import fs2._
import fs2.async.mutable.Queue
import org.apache.kafka.common.TopicPartition

object RecordStream {
  case class Partitioned[F[_], T](
    commitQueue: CommitQueue[F],
    records: Stream[F, (TopicPartition, Stream[F, ConsumerMessage[Result, T]])])
  case class Plain[F[_], T](commitQueue: CommitQueue[F],
                            records: Stream[F, ConsumerMessage[Result, T]])

  case class PartitionHandle[F[_]](recordCount: Ref[F, Int],
                                   data: async.mutable.Queue[F, Option[Chunk[ByteRecord]]]) {
    def enqueue(chunk: Chunk[ByteRecord])(implicit F: Apply[F]): F[Unit] =
      recordCount.update(_ + chunk.size) *>
        data.enqueue1(chunk.some)

    def complete: F[Unit] = data.enqueue1(none)

    def dequeue(implicit F: Functor[F]): Stream[F, ByteRecord] =
      data.dequeue.unNoneTerminate
        .evalMap { chunk =>
          recordCount.update(_ - chunk.size).as(chunk)
        }
        .flatMap(Stream.chunk(_))
  }

  object PartitionHandle {
    def create[F[_]: Concurrent]: F[PartitionHandle[F]] =
      for {
        recordCount <- Ref[F].of(0)
        queue <- async
                  .unboundedQueue[F, Option[Chunk[ByteRecord]]]
      } yield PartitionHandle(recordCount, queue)
  }

  case class Resources[F[_], T](
    consumer: Consumer[F],
    polls: Stream[F, Poll.type],
    commits: CommitQueue[F],
    shutdownQueue: Queue[F, None.type],
    partitionTracker: Ref[F, Map[TopicPartition, PartitionHandle[F]]],
    pausedPartitions: Ref[F, Set[TopicPartition]],
    pendingRebalances: Ref[F, List[Rebalance]],
    partitionsQueue: Queue[
      F,
      Either[Throwable, Option[(TopicPartition, Stream[F, ConsumerMessage[Result, T]])]]]
  ) {
    def commandStream(implicit F: Concurrent[F])
      : Stream[F, Either[(Deferred[F, Either[Throwable, Unit]], CommitRequest), Poll.type]] =
      shutdownQueue.dequeue
        .mergeHaltL(commits.queue.dequeue.either(polls).map(_.some))
        .unNoneTerminate
  }

  def applyRebalanceEvents[F[_]: Concurrent, T: KafkaDecoder](
    partitionTracker: Ref[F, Map[TopicPartition, PartitionHandle[F]]],
    partitionsQueue: Queue[
      F,
      Either[Throwable, Option[(TopicPartition, Stream[F, ConsumerMessage[Result, T]])]]],
    rebalances: List[Rebalance]): F[Unit] = rebalances.traverse_ {
    case Rebalance.Assign(partitions) =>
      for {
        tracker <- partitionTracker.get
        handles <- partitions.traverse(PartitionHandle.create.tupleLeft(_))
        _       <- partitionTracker.set(tracker ++ handles)
        _ <- handles.traverse_ {
              case (tp, h) =>
                partitionsQueue.enqueue1((tp, h.dequeue through deserialize[F, T]).some.asRight)
            }
      } yield ()
    case Rebalance.Revoke(partitions) =>
      for {
        tracker <- partitionTracker.get
        handles = partitions.flatMap(tracker.get)
        _ <- handles.traverse_(_.complete)
        _ <- partitionTracker.set(tracker -- partitions)
      } yield ()

  }

  def resumePartitions[F[_]](
    settings: ConsumerSettings,
    pausedPartitions: Set[TopicPartition],
    partitionTracker: Map[TopicPartition, PartitionHandle[F]]
  )(implicit F: Concurrent[F]): F[List[TopicPartition]] =
    pausedPartitions.toList.flatTraverse { tp =>
      partitionTracker.get(tp) match {
        case Some(handle) =>
          handle.recordCount.get.map { count =>
            if (count <= settings.partitionOutputBufferSize)
              List(tp)
            else Nil
          }
        case None =>
          F.raiseError[List[TopicPartition]](new Exception)
      }
    }

  def distributeRecords[F[_]](settings: ConsumerSettings,
                              partitionTracker: Map[TopicPartition, PartitionHandle[F]],
                              records: Map[TopicPartition, List[ByteRecord]])(
    implicit F: Concurrent[F]): F[List[TopicPartition]] =
    records.toList.flatTraverse {
      case (tp, records) =>
        partitionTracker.get(tp) match {
          case Some(handle) =>
            for {
              _           <- handle.enqueue(Chunk.seq(records))
              recordCount <- handle.recordCount.get
              shouldPause = if (recordCount <= settings.partitionOutputBufferSize)
                Nil
              else
                List(tp)
            } yield shouldPause
          case None =>
            F.raiseError[List[TopicPartition]](new Exception("Got records for untracked partition"))
        }
    }

  def commandHandler[F[_], T: KafkaDecoder](
    resources: Resources[F, T],
    settings: ConsumerSettings,
    command: Either[(Deferred[F, Either[Throwable, Unit]], CommitRequest), Poll.type])(
    implicit F: Concurrent[F]): F[Unit] =
    command match {
      case Left((deferred, req)) =>
        (resources.consumer
          .commit(req.offsets)
          .void
          .attempt >>= deferred.complete).void
      case Right(Poll) =>
        for {
          resumablePartitions <- for {
                                  paused    <- resources.pausedPartitions.get
                                  tracker   <- resources.partitionTracker.get
                                  resumable <- resumePartitions(settings, paused, tracker)
                                } yield resumable
          _ <- resources.consumer.resume(resumablePartitions) *>
                resources.pausedPartitions.update(_ -- resumablePartitions)

          records    <- resources.consumer.poll(settings.pollTimeout, settings.wakeupTimeout)
          rebalances <- resources.pendingRebalances.getAndSet(Nil).map(_.reverse)

          _ <- applyRebalanceEvents(
                resources.partitionTracker,
                resources.partitionsQueue,
                rebalances
              )

          partitionsToPause <- for {
                                tracker           <- resources.partitionTracker.get
                                partitionsToPause <- distributeRecords(settings, tracker, records)
                              } yield partitionsToPause
          _ <- resources.consumer.pause(partitionsToPause) *>
                resources.pausedPartitions.update(_ ++ partitionsToPause)
        } yield ()
    }

  def pollingLoop[F[_], T: KafkaDecoder](resources: Resources[F, T], settings: ConsumerSettings)(
    implicit F: Concurrent[F]) =
    resources.commandStream
      .evalMap(commandHandler(resources, settings, _))

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

      resources <- Resource.liftF {
                    for {
                      partitionTracker <- Ref[F].of(Map.empty[TopicPartition, PartitionHandle[F]])
                      partitionsQueue <- async.unboundedQueue[
                                          F,
                                          Either[Throwable,
                                                 Option[(TopicPartition,
                                                         Stream[F, ConsumerMessage[Result, T]])]]]
                      pausedPartitions <- Ref[F].of(Set.empty[TopicPartition])
                      commitQueue      <- CommitQueue.create[F](settings.maxPendingCommits)
                      shutdownQueue    <- async.boundedQueue[F, None.type](1)
                      polls = Stream(Poll) ++ Stream.fixedRate(settings.pollInterval).as(Poll)

                    } yield
                      Resources(
                        consumer,
                        polls,
                        commitQueue,
                        shutdownQueue,
                        partitionTracker,
                        pausedPartitions,
                        pendingRebalances,
                        partitionsQueue)
                  }

      partitionsOut = resources.partitionsQueue.dequeue.rethrow.unNoneTerminate

      _ <- Resource.make {
            pollingLoop(resources, settings).compile.drain.start
          }(fiber => resources.shutdownQueue.enqueue1(None) *> fiber.join)

    } yield Partitioned(resources.commits, partitionsOut)

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
