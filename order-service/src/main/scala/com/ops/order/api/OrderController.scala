package com.ops.order.api

import org.apache.pekko.http.scaladsl.model.StatusCodes
import org.apache.pekko.http.scaladsl.server.Directives.*
import org.apache.pekko.http.scaladsl.server.Route
import com.ops.order.api.dto.*
import com.ops.order.service.{DuplicateRequest, InternalError, InvalidTransition, NotFound, OrderService, ValidationError}
import com.github.pjfanning.pekkohttpcirce.FailFastCirceSupport.*
import io.circe.generic.auto.*
import io.circe.syntax.*
import java.time.Instant
import scala.concurrent.ExecutionContext
import scala.util.{Failure, Success}

// ── OrderController — ENDPOINT LAYER ─────────────────────────────────────────
// Responsibilities: unmarshal, validate, call service, marshal response.
// NO business logic here.
class OrderController(service: OrderService)(using ec: ExecutionContext) {

  val routes: Route = pathPrefix("orders") {
    concat(
      // POST /orders — create
      (post & pathEndOrSingleSlash) {
        (headerValueByName("X-Trace-Id") & entity(as[CreateOrderRequest])) { (traceId, req) =>
          validate(req.items.nonEmpty, "items must not be empty") {
            validate(req.customerId.nonEmpty, "customerId is required") {
              onComplete(service.createOrder(req, traceId)) {
                case Success(Right(resp))                      => complete(StatusCodes.Created, resp)
                case Success(Left(InvalidTransition(reason))) => complete(StatusCodes.Conflict, error("CONFLICT", reason, traceId))
                case Success(Left(DuplicateRequest(id)))       => complete(StatusCodes.Conflict, error("DUPLICATE_ORDER", s"Order $id already exists", traceId))
                case Success(Left(ValidationError(msg)))      => complete(StatusCodes.BadRequest, error("INVALID_REQUEST", msg, traceId))
                case Success(Left(InternalError(msg)))        => complete(StatusCodes.InternalServerError, error("INTERNAL_ERROR", msg, traceId))
                case Success(Left(NotFound(id)))              => complete(StatusCodes.NotFound, error("NOT_FOUND", s"Order $id not found", traceId))
                case Failure(ex)                              => complete(StatusCodes.InternalServerError, error("INTERNAL_ERROR", ex.getMessage, traceId))
              }
            }
          }
        }
      },
      // GET /orders/{id}
      (get & path(Segment)) { id =>
        headerValueByName("X-Trace-Id") { traceId =>
          onComplete(service.getOrder(id)) {
            case Success(Some(resp)) => complete(StatusCodes.OK, resp)
            case Success(None)       => complete(StatusCodes.NotFound, error("NOT_FOUND", s"Order $id not found", traceId))
            case Failure(ex)         => complete(StatusCodes.InternalServerError, error("INTERNAL_ERROR", ex.getMessage, traceId))
          }
        }
      },
      // GET /orders?customerId=...&page=1&pageSize=20
      (get & pathEndOrSingleSlash & parameters("customerId", "page".as[Int].withDefault(1), "pageSize".as[Int].withDefault(20))) {
        (customerId, page, pageSize) =>
          headerValueByName("X-Trace-Id") { traceId =>
            validate(page >= 1, "page must be >= 1") {
              validate(pageSize >= 1 && pageSize <= 100, "pageSize must be 1–100") {
                onComplete(service.listOrders(customerId, page, pageSize)) {
                  case Success(resp) => complete(StatusCodes.OK, resp)
                  case Failure(ex)   => complete(StatusCodes.InternalServerError, error("INTERNAL_ERROR", ex.getMessage, traceId))
                }
              }
            }
          }
      },
      // DELETE /orders/{id}  — cancel
      (delete & path(Segment)) { id =>
        (headerValueByName("X-Trace-Id") & entity(as[CancelOrderRequest])) { (traceId, req) =>
          onComplete(service.cancelOrder(id, req.reason, traceId)) {
            case Success(Right(resp))                      => complete(StatusCodes.OK, resp)
            case Success(Left(NotFound(_)))                => complete(StatusCodes.NotFound, error("NOT_FOUND", s"Order $id not found", traceId))
            case Success(Left(InvalidTransition(reason))) => complete(StatusCodes.Conflict, error("INVALID_TRANSITION", reason, traceId))
            case Success(Left(err))                       => complete(StatusCodes.BadRequest, error("BAD_REQUEST", err.toString, traceId))
            case Failure(ex)                              => complete(StatusCodes.InternalServerError, error("INTERNAL_ERROR", ex.getMessage, traceId))
          }
        }
      },
      // POST /orders/{id}/return — request return
      (post & path(Segment / "return")) { id =>
        (headerValueByName("X-Trace-Id") & entity(as[RequestReturnRequest])) { (traceId, req) =>
          onComplete(service.requestReturn(id, req.reason, traceId)) {
            case Success(Right(resp))                      => complete(StatusCodes.OK, resp)
            case Success(Left(NotFound(_)))                => complete(StatusCodes.NotFound, error("NOT_FOUND", s"Order $id not found", traceId))
            case Success(Left(InvalidTransition(reason))) => complete(StatusCodes.Conflict, error("INVALID_TRANSITION", reason, traceId))
            case Success(Left(err))                       => complete(StatusCodes.BadRequest, error("BAD_REQUEST", err.toString, traceId))
            case Failure(ex)                              => complete(StatusCodes.InternalServerError, error("INTERNAL_ERROR", ex.getMessage, traceId))
          }
        }
      },
      // PUT /orders/{id}/return/approve
      (put & path(Segment / "return" / "approve")) { id =>
        headerValueByName("X-Trace-Id") { traceId =>
          onComplete(service.approveReturn(id, traceId)) {
            case Success(Right(resp))                      => complete(StatusCodes.OK, resp)
            case Success(Left(NotFound(_)))                => complete(StatusCodes.NotFound, error("NOT_FOUND", s"Order $id not found", traceId))
            case Success(Left(InvalidTransition(reason))) => complete(StatusCodes.Conflict, error("INVALID_TRANSITION", reason, traceId))
            case Success(Left(err))                       => complete(StatusCodes.BadRequest, error("BAD_REQUEST", err.toString, traceId))
            case Failure(ex)                              => complete(StatusCodes.InternalServerError, error("INTERNAL_ERROR", ex.getMessage, traceId))
          }
        }
      },
      // PUT /orders/{id}/return/reject
      (put & path(Segment / "return" / "reject")) { id =>
        (headerValueByName("X-Trace-Id") & entity(as[RejectReturnRequest])) { (traceId, req) =>
          onComplete(service.rejectReturn(id, req.reason, traceId)) {
            case Success(Right(resp))                      => complete(StatusCodes.OK, resp)
            case Success(Left(NotFound(_)))                => complete(StatusCodes.NotFound, error("NOT_FOUND", s"Order $id not found", traceId))
            case Success(Left(InvalidTransition(reason))) => complete(StatusCodes.Conflict, error("INVALID_TRANSITION", reason, traceId))
            case Success(Left(err))                       => complete(StatusCodes.BadRequest, error("BAD_REQUEST", err.toString, traceId))
            case Failure(ex)                              => complete(StatusCodes.InternalServerError, error("INTERNAL_ERROR", ex.getMessage, traceId))
          }
        }
      },
      // GET /health — liveness probe
      (get & path("health")) {
        complete(StatusCodes.OK, """{"status":"UP"}""")
      }
    )
  }

  private def error(code: String, msg: String, traceId: String) =
    ErrorResponse(code, msg, traceId)
}
