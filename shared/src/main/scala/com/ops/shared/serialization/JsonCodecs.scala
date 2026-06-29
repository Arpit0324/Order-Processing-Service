package com.ops.shared.serialization

import com.ops.shared.domain.{ItemLine, Money, ShippingAddress}
import com.ops.shared.events.*
import io.circe.{Codec, Decoder, Encoder}
import io.circe.generic.semiauto.*
import io.circe.syntax.*
import java.time.Instant

// ── JSON codecs for all shared types ─────────────────────────────────────────
// Used by Scala services directly.
// Java services (api-gateway, notification-service) use Jackson — keep JSON
// field names identical so the wire format stays compatible.

object JsonCodecs {

  // Instant as ISO-8601 string
  given Encoder[Instant] = Encoder.encodeString.contramap(_.toString)
  given Decoder[Instant] = Decoder.decodeString.emapTry(s => scala.util.Try(Instant.parse(s)))

  // Value objects
  given Codec[Money]           = deriveCodec
  given Codec[ItemLine]        = deriveCodec
  given Codec[ShippingAddress] = deriveCodec

  // Events
  given Codec[OrderCreatedEvent]        = deriveCodec
  given Codec[OrderCancelledEvent]      = deriveCodec
  given Codec[OrderCancelRequestedEvent]= deriveCodec
  given Codec[OrderReturnRequestedEvent]= deriveCodec
  given Codec[OrderReturnedEvent]       = deriveCodec
  given Codec[InventoryUpdatedEvent]    = deriveCodec
  given Codec[NotificationSentEvent]    = deriveCodec
  given Codec[RefundRequestedEvent]     = deriveCodec
}
