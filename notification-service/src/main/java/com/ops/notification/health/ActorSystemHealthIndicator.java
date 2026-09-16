package com.ops.notification.health;

import org.apache.pekko.actor.typed.ActorSystem;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.stereotype.Component;

// ── Reports the Pekko actor system in the readiness/liveness probe ───────────
// If the system has terminated (unrecoverable), the pod should be restarted /
// pulled from the load balancer — a running JVM with a dead actor system can
// no longer deliver notifications.
@Component("actorSystem")
public class ActorSystemHealthIndicator implements HealthIndicator {

    private final ActorSystem<?> system;

    public ActorSystemHealthIndicator(ActorSystem<?> system) {
        this.system = system;
    }

    @Override
    public Health health() {
        boolean terminated = system.getWhenTerminated().toCompletableFuture().isDone();
        if (terminated) {
            return Health.down().withDetail("actorSystem", system.name() + " terminated").build();
        }
        return Health.up().withDetail("actorSystem", system.name()).build();
    }
}
