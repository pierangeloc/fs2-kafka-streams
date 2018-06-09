package com.iravid.fs2.kafka.client

import cats.effect.Resource
import cats.effect.concurrent.{ Deferred, Ref }
import cats.effect.{ Concurrent, ConcurrentEffect, Sync, Timer }
import cats.implicits._
import com.iravid.fs2.kafka.EnvT
import com.iravid.fs2.kafka.codecs.KafkaDecoder
import com.iravid.fs2.kafka.model.{ ByteRecord, ConsumerMessage, Result }
import fs2.{ async, Pipe, Segment, Stream }

case class RecordStream[F[_], T](commitQueue: CommitQueue[F],
                                 records: Stream[F, ConsumerMessage[Result, T]])

object RecordStream {
  def resources[F[_], T: KafkaDecoder](settings: ConsumerSettings, consumer: Consumer[F])(
    implicit F: ConcurrentEffect[F],
    timer: Timer[F]) =
    for {
      commitQueue         <- CommitQueue.create[F](settings.maxPendingCommits)
      outQueue            <- async.unboundedQueue[F, (Long, Segment[ByteRecord, Unit])]
      currentBufferSize   <- Ref[F].of(0L)
      pollingLoopShutdown <- Deferred[F, Either[Throwable, Unit]]
      commits = commitQueue.queue.dequeue
      polls   = Stream(Poll) ++ Stream.repeatEval(timer.sleep(settings.pollInterval).as(Poll))
      _ <- Concurrent[F].start {
            commits
              .either(polls)
              .interruptWhen(pollingLoopShutdown.get)
              .evalMap {
                case Left((deferred, req)) =>
                  (consumer.commit(req.asOffsetMap).void.attempt >>= deferred.complete).void
                case Right(Poll) =>
                  for {
                    buf <- currentBufferSize.get
                    _ <- if (buf >= settings.outputBufferSize) F.unit
                        else
                          for {
                            records <- consumer.poll(settings.pollTimeout, settings.wakeupTimeout)
                            recordCount = records.size.toLong
                            _ <- currentBufferSize.update(_ + records.size)
                            _ <- outQueue.enqueue1((recordCount, Segment.seq(records)))
                          } yield ()
                  } yield ()
              }
              .compile
              .drain
          }
      outputStream = outQueue.dequeue
        .evalMap {
          case (size, segment) =>
            currentBufferSize.update(_ - size).as(segment)
        }
        .flatMap(Stream.segment(_).covary[F])
        .through(deserialize[F, T])
    } yield (commitQueue, outputStream, pollingLoopShutdown.complete(Right(())))

  def apply[F[_]: ConcurrentEffect, T: KafkaDecoder](
    settings: ConsumerSettings,
    consumer: Consumer[F],
    subscription: Subscription)(implicit timer: Timer[F]): Resource[F, RecordStream[F, T]] =
    Resource.make(resources[F, T](settings, consumer)) {
      case (_, _, shutdown) =>
        for {
          _ <- consumer.unsubscribe
          _ <- shutdown
        } yield ()
    } flatMap {
      case (commitQueue, outputStream, _) =>
        Resource.liftF {
          consumer
            .subscribe(subscription, _ => Sync[F].unit)
            .as(RecordStream(commitQueue, outputStream))
        }
    }

  def deserialize[F[_], T: KafkaDecoder]: Pipe[F, ByteRecord, ConsumerMessage[Result, T]] =
    _.map(rec => EnvT(rec, KafkaDecoder[T].decode(rec)))
}
