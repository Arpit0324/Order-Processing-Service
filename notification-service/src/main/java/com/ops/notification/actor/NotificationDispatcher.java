package com.ops.notification.actor;

import org.apache.pekko.actor.typed.ActorRef;
import org.apache.pekko.actor.typed.ActorSystem;
import org.apache.pekko.actor.typed.Scheduler;
import org.apache.pekko.actor.typed.javadsl.AskPattern;
import com.ops.notification.service.NotificationDeliveryException;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

// ── Synchronous facade over the actor router ─────────────────────────────────
// Wraps the ask pattern so the service layer gets an explicit success/failure
// result instead of fire-and-forget. Mocked in unit tests.
@Component
public class NotificationDispatcher {

    private static final Duration ASK_TIMEOUT = Duration.ofSeconds(10);

    private final ActorRef<NotificationCommand> router;
    private final Scheduler                     scheduler;

    public NotificationDispatcher(ActorRef<NotificationCommand> router,
                                  ActorSystem<?> system) {
        this.router    = router;
        this.scheduler = system.scheduler();
    }

    public DeliveryResult dispatch(Function<ActorRef<DeliveryResult>, NotificationCommand> messageFactory) {
        try {
            return AskPattern.ask(router, messageFactory::apply, ASK_TIMEOUT, scheduler)
                    .toCompletableFuture()
                    .get(ASK_TIMEOUT.plusSeconds(5).toMillis(), TimeUnit.MILLISECONDS);
        } catch (Exception ex) {
            throw new NotificationDeliveryException(
                    "Actor dispatch failed or timed out: " + ex.getMessage(), ex);
        }
    }
}
