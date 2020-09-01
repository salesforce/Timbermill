package com.datorama.oss.timbermill.pipe

import java.util.UUID

import akka.NotUsed
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.http.scaladsl.unmarshalling.Unmarshal
import akka.stream.scaladsl.{RestartSource, Sink, Source}
import akka.stream.{Materializer, OverflowStrategy, QueueOfferResult}
import akka.util.ByteString
import com.datorama.oss.timbermill.unit.Event
import com.fasterxml.jackson.databind.ObjectMapper
import com.typesafe.scalalogging.Logger

import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration
import scala.util.{Failure, Success, Try}

class AkkaPipe(timbermillServerUrl : String, maxBufferSize: Long, maxEventsBatchSize: Long, maxSecondsBeforeBatchTimeout: FiniteDuration)(implicit mat : Materializer) extends EventOutputPipe {
  import scala.concurrent.duration._
  val log = Logger("com.datorama.pluto.exec.TimbermillInitializer.AkkaPipe")
  val objMapper = new ObjectMapper()
  val tmServerUri = {
    timbermillServerUrl + "/events"
  }
  val queue = Source
    .queue[Event](5, OverflowStrategy.dropNew, 4/*num cpus / 2*/)
    .map(objMapper.writeValueAsBytes) //Event.estimateSize is broken (it will return negatives when a metric has a zero value) so we transform the events into json strings very early in the game and rely on ByteString.size for weight function
    .map(ByteString.apply)
    //this will backpressure the queue, it accumulates events into batches as long as there's no downstream demand and the batch's weight is within bounds
    //notice the queue has buffer size of 5 + a 'slack' of 4 concurrent entries, so backpressure applies after exhausting this buffer
    //it's also important to understand that messages are queued into the underlying actor's mailbox, so theoretically it's possible for few more messages to be buffered there
    .batchWeighted(maxBufferSize, _.size, Vector.newBuilder += _)(_ += _)
    .mapConcat(_.result())
    //collect events and cur them into requests by size or time
    .groupedWeightedWithin(maxEventsBatchSize, maxSecondsBeforeBatchTimeout)(_.size)
    .collect{
      case b if b.nonEmpty =>
        //construct the json for the event wrapper, since it's a simple object and we've already serialized the events separately we do this by string concatenation.
        val jsArrString = ByteString('[') ++
          b
            .tail
            .foldLeft(b.head){
              case (prev, x) => prev ++ ByteString(',') ++ x
            } ++
          ByteString(']')
        val id = UUID.randomUUID.toString
        val jsMsg = ByteString(s"""{"@type": "EventsWrapper", "id": "$id", "events": """) ++
          jsArrString ++
          ByteString('}')
        log.debug(s"request size: ${jsMsg.size}")
        val httpReq = HttpRequest(
          method = HttpMethods.POST,
          uri = tmServerUri,
          entity = HttpEntity(ContentTypes.`application/json`, jsMsg)
        )
        (httpReq, id)
    }
    .flatMapConcat{
      case (req, reqId) => sendRequest(req, reqId)
    }
    .to{
      Sink.foreach{
        case Success(resp) => resp.discardEntityBytes()
        case Failure(ex) =>
          log.error("failed sending request to TM server (exhausted retries)", ex)
      }
    }
    .run

  def sendRequest(req : HttpRequest, reqId : String): Source[Try[HttpResponse], NotUsed] = {
    log.debug(s"sending request $reqId\n$req")
    RestartSource.withBackoff(
      minBackoff = 1000.milliseconds, //todo derive from tm client's impl
      maxBackoff = 1000.milliseconds,
      randomFactor = 0.2,
      maxRestarts = 5
    ){() =>
      Source
        .single(req)
        .mapAsync(1)(Http(mat.system).singleRequest(_)) //todo, set timeout
        .mapAsync(1){
          case resp if resp.status.isSuccess() => Future successful resp
          case resp =>  //todo, we might want to suppress retries for some failures
            import mat.executionContext
            //import akka.http.scaladsl.unmarshalling.Unmarshaller
            resp
              .toStrict(2 seconds)
              .flatMap {
                Unmarshal(_)
                  .to[String]
                  .map{errMsg =>
                    log.warn(s"Failed attempt to send request $reqId to TM server")
                    sys error s"TM request [${reqId}] fail"
                  }
              }
        }
    }
      .map(Success.apply)
      .recover(Failure.apply[HttpResponse] _)
  }

  override def send(e: Event): Unit = {
    import mat.executionContext

    queue
      .offer(e)
      .foreach{
        case QueueOfferResult.Enqueued =>
          log.info(s"Event ${e.getTaskId} successfully enqueued")
        case QueueOfferResult.Dropped =>
          log.warn(s"Event ${e.getTaskId} was removed from the queue due to insufficient space")
        case QueueOfferResult.Failure(ex) =>
          log.warn("failed enqueueing event ${e.getTaskId}", ex)
        case QueueOfferResult.QueueClosed =>
          log.warn(s"Event ${e.getTaskId} was attempted to be enqueued after the queue has been closed")
      }
  }

  override def getCurrentBufferSize: Int = 42 //can't have this

  override def close(): Unit = {
    log.info("closing queue")
    queue.complete()
  }
}