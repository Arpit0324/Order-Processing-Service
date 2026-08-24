package com.ops.inventory.api

import org.apache.pekko.http.scaladsl.model.StatusCodes
import org.apache.pekko.http.scaladsl.server.Directives.*
import org.apache.pekko.http.scaladsl.server.Route
import com.ops.inventory.api.dto.*
import com.ops.inventory.service.InventoryService
import com.github.pjfanning.pekkohttpcirce.FailFastCirceSupport.*
import io.circe.generic.auto.*
import org.slf4j.LoggerFactory
import scala.concurrent.ExecutionContext
import scala.util.{Failure, Success}

class InventoryController(service: InventoryService)(using ec: ExecutionContext) {

  private val log = LoggerFactory.getLogger(getClass)

  val routes: Route = concat(
    (get & path("health")) {
      complete(StatusCodes.OK, """{"status":"UP"}""")
    },

    pathPrefix("inventory") {
      concat(

        // GET /inventory?page=1&pageSize=20
        (get & pathEndOrSingleSlash &
          parameters("page".as[Int].withDefault(1), "pageSize".as[Int].withDefault(20))) {
          (page, pageSize) =>
            // Fix: clamp page/pageSize to prevent negative OFFSET or huge scans
            val safePage     = math.max(1, page)
            val safePageSize = math.min(math.max(1, pageSize), 100)
            headerValueByName("X-Trace-Id") { traceId =>
              onComplete(service.listItems(safePage, safePageSize)) {
                case Success((items, total)) =>
                  complete(StatusCodes.OK, InventoryListResponse(items, total, safePage, safePageSize))
                case Failure(ex) =>
                  log.error("listItems failed traceId={}", traceId, ex)
                  complete(StatusCodes.InternalServerError, error("INTERNAL_ERROR", ex.getMessage, traceId))
              }
            }
        },

        // GET /inventory/{productId}
        (get & path(Segment)) { productId =>
          headerValueByName("X-Trace-Id") { traceId =>
            onComplete(service.getItem(productId)) {
              case Success(Some(resp)) => complete(StatusCodes.OK, resp)
              case Success(None)       => complete(StatusCodes.NotFound, error("NOT_FOUND", s"Product $productId not found", traceId))
              case Failure(ex)         => complete(StatusCodes.InternalServerError, error("INTERNAL_ERROR", ex.getMessage, traceId))
            }
          }
        },

        // PUT /inventory/{productId}/stock -- manual stock adjustment
        (put & path(Segment / "stock") & entity(as[UpdateStockRequest])) { (productId, req) =>
          headerValueByName("X-Trace-Id") { traceId =>
            validate(req.delta != 0, "delta must be non-zero") {
              onComplete(service.updateStock(productId, req.delta, traceId)) {
                case Success(Some(resp)) => complete(StatusCodes.OK, resp)
                case Success(None)       => complete(StatusCodes.NotFound, error("NOT_FOUND", s"Product $productId not found", traceId))
                case Failure(ex)         => complete(StatusCodes.InternalServerError, error("INTERNAL_ERROR", ex.getMessage, traceId))
              }
            }
          }
        },

        // POST /inventory/reserve -- Fix C: actually call reserveForOrder, not getItem
        (post & path("reserve") & entity(as[ReserveRequest])) { req =>
          headerValueByName("X-Trace-Id") { traceId =>
            validate(req.quantity > 0, "quantity must be > 0") {
              onComplete(service.reserveItem(req.productId, req.orderId, req.quantity, traceId)) {
                case Success(Right(resp))   => complete(StatusCodes.OK, resp)
                case Success(Left("NOT_FOUND")) =>
                  complete(StatusCodes.NotFound, error("NOT_FOUND", s"Product ${req.productId} not found", traceId))
                case Success(Left(reason)) =>
                  complete(StatusCodes.UnprocessableEntity, error(reason, reason, traceId))
                case Failure(ex) =>
                  complete(StatusCodes.InternalServerError, error("INTERNAL_ERROR", ex.getMessage, traceId))
              }
            }
          }
        }
      )
    }
  )

  private def error(code: String, msg: String, traceId: String) =
    ErrorResponse(code, msg, traceId)
}
