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

// ── InventoryController — ENDPOINT LAYER ─────────────────────────────────────
class InventoryController(service: InventoryService)(using ec: ExecutionContext) {

  private val log = LoggerFactory.getLogger(getClass)

  val routes: Route = concat(
    // Health probe
    (get & path("health")) {
      complete(StatusCodes.OK, """{"status":"UP"}""")
    },

    pathPrefix("inventory") {
      concat(

        // GET /inventory?page=1&pageSize=20
        (get & pathEndOrSingleSlash &
          parameters("page".as[Int].withDefault(1), "pageSize".as[Int].withDefault(20))) {
          (page, pageSize) =>
            headerValueByName("X-Trace-Id") { traceId =>
              onComplete(service.listItems(page, pageSize)) {
                case Success((items, total)) =>
                  complete(StatusCodes.OK, InventoryListResponse(items, total, page, pageSize))
                case Failure(ex) =>
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

        // PUT /inventory/{productId}/stock  — manual stock adjustment
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

        // POST /inventory/reserve  — direct reservation (used by integration tests)
        (post & path("reserve") & entity(as[ReserveRequest])) { req =>
          headerValueByName("X-Trace-Id") { traceId =>
            validate(req.quantity > 0, "quantity must be > 0") {
              onComplete(service.getItem(req.productId)) {
                case Success(Some(item)) if item.availableQty >= req.quantity =>
                  complete(StatusCodes.OK, item)
                case Success(Some(item)) =>
                  complete(StatusCodes.UnprocessableEntity,
                    error("INSUFFICIENT_STOCK",
                          s"Only ${item.availableQty} units available, requested ${req.quantity}", traceId))
                case Success(None) =>
                  complete(StatusCodes.NotFound, error("NOT_FOUND", s"Product ${req.productId} not found", traceId))
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
