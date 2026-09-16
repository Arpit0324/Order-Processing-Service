package com.ops.notification.config;

import org.apache.pekko.actor.typed.ActorRef;
import org.apache.pekko.actor.typed.ActorSystem;
import com.ops.notification.actor.NotificationCommand;
import com.ops.notification.actor.NotificationRouter;
import com.ops.notification.provider.NotificationProvider;
import com.typesafe.config.ConfigFactory;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

// ── Single Spring-managed ActorSystem; the router IS the root guardian ───────
// Previously this config created two ActorSystems (one Behaviors.empty(), one
// for the router) — the router's lifecycle was never tied to Spring shutdown.
@Configuration
public class AkkaConfig {

    @Bean(destroyMethod = "terminate")
    public ActorSystem<NotificationCommand> actorSystem(
            NotificationProvider provider,
            MeterRegistry registry,
            @Value("${notification.actor.email-pool-size:4}") int emailPoolSize,
            @Value("${notification.actor.sms-pool-size:2}") int smsPoolSize) {
        return ActorSystem.create(
                NotificationRouter.create(provider, registry, emailPoolSize, smsPoolSize),
                "notification-system", ConfigFactory.load().getConfig("pekko"));
    }

    @Bean
    public ActorRef<NotificationCommand> notificationRouter(ActorSystem<NotificationCommand> system) {
        return system; // root guardian is the router
    }
}
