package com.ops.notification.config;

import org.apache.pekko.actor.typed.ActorRef;
import org.apache.pekko.actor.typed.ActorSystem;
import org.apache.pekko.actor.typed.javadsl.Behaviors;
import com.ops.notification.actor.NotificationCommand;
import com.ops.notification.actor.NotificationRouter;
import com.typesafe.config.ConfigFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

// ── Akka ActorSystem and router actor as Spring beans ─────────────────────────
@Configuration
public class AkkaConfig {

    @Bean(destroyMethod = "terminate")
    public ActorSystem<Void> actorSystem() {
        return ActorSystem.create(Behaviors.empty(), "notification-system",
                ConfigFactory.load().getConfig("pekko"));
    }

    @Bean
    public ActorRef<NotificationCommand> notificationRouter(ActorSystem<Void> system) {
        return ActorSystem.create(NotificationRouter.create(), "notification-router");
    }
}
