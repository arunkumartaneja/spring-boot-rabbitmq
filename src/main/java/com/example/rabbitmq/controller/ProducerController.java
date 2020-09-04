package com.example.rabbitmq.controller;

import com.example.rabbitmq.RabbitmqApplication;
import com.example.rabbitmq.config.DLXParkingLotAmqpConfiguration;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

import static com.example.rabbitmq.config.BroadcastConfig.FANOUT_EXCHANGE_NAME;
import static com.example.rabbitmq.config.BroadcastConfig.TOPIC_EXCHANGE_NAME;

@RestController
public class ProducerController {

    @Autowired
    private RabbitTemplate template;

    private static final String ROUTING_KEY_USER_IMPORTANT_WARN = "user.important.warn";
    private static final String ROUTING_KEY_USER_IMPORTANT_ERROR = "user.important.error";

    /**
     * RabbitMQ will send each message to the next consumer, in sequence. On average every consumer will get the same number of messages.
     * This way of distributing messages is called round-robin
     * non round-robbin property spring.rabbitmq.listener.simple.prefetch=2
     *
     * @param message
     * @return
     */
    @GetMapping("msg/{msg}")
    public String sendMsg(@PathVariable("msg") String message) {
        System.out.println("msg  -> " + message);
        for (int i = 0; i < 10; i++) {
            this.template.convertAndSend(RabbitmqApplication.MY_QUEUE_NAME, message);
        }

        System.out.println("Sent '" + message + "'");
        return "Sent '" + message + "'";
    }

    /**
     * broadcast message using fanout exchange
     *
     * @param message
     * @return
     */
    @GetMapping("broadcast/{msg}")
    public String broadcast(@PathVariable("msg") String message) {
        System.out.println("msg  -> " + message);
        for (int i = 0; i < 5; i++) {
            this.template.convertAndSend(FANOUT_EXCHANGE_NAME, "", "fanout" + message + i);
            this.template.convertAndSend(TOPIC_EXCHANGE_NAME, ROUTING_KEY_USER_IMPORTANT_WARN, "topic important warn" + message + i);
            this.template.convertAndSend(TOPIC_EXCHANGE_NAME, ROUTING_KEY_USER_IMPORTANT_ERROR, "topic important error" + message + i);
        }

        System.out.println("Sent '" + message + "'");
        return "Sent '" + message + "'";
    }

    /**
     * error handling using DLQ, parking lot queue
     *
     * @param message
     * @return
     */
    @GetMapping("error-handler/{msg}")
    public String errorHandler(@PathVariable("msg") String message) {
        this.template.convertAndSend(DLXParkingLotAmqpConfiguration.MESSAGES_EXCHANGE,
                DLXParkingLotAmqpConfiguration.ROUTING_KEY_MESSAGES_QUEUE, message);
        System.out.println("Sent '" + message + "'");
        return "Sent '" + message + "'";
    }

}
