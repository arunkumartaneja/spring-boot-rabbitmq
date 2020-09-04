package com.example.rabbitmq;

import com.example.rabbitmq.config.DLXParkingLotAmqpConfiguration;
import com.example.rabbitmq.errorhandler.BusinessException;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

import static com.example.rabbitmq.config.BroadcastConfig.*;

@SpringBootApplication
public class RabbitmqApplication {

    private static final boolean NON_DURABLE = false;
    public static final String MY_QUEUE_NAME = "myQueue";
    public static final int MAX_RETRIES_COUNT = 2;
    public static final String HEADER_X_RETRIES_COUNT = "x-retries-count";

    @Autowired
    private RabbitTemplate template;

    public static void main(String[] args) {
        SpringApplication.run(RabbitmqApplication.class, args);
    }

    @Bean
    public Queue myQueue() {
        Queue queue = new Queue(MY_QUEUE_NAME, NON_DURABLE);
        return queue;
    }

    @RabbitListener(queues = MY_QUEUE_NAME)
    public void listen1(String in) {
        System.out.println("Message read by listen1 from myQueue : " + in);
    }

    @RabbitListener(queues = MY_QUEUE_NAME)
    public void listen2(String in) throws InterruptedException {
        // sleep thread to keep consumer bzy
        Thread.sleep(3000);
        System.out.println("Message read by listen2 from myQueue : " + in);
    }

    @RabbitListener(queues = {FANOUT_QUEUE_1_NAME})
    public void receiveMessageFromFanout1(String message) {
        System.out.println("Received fanout 1 message: " + message);
    }

    @RabbitListener(queues = {FANOUT_QUEUE_2_NAME})
    public void receiveMessageFromFanout2(String message) {
        System.out.println("Received fanout 2 message: " + message);
    }

    @RabbitListener(queues = {TOPIC_QUEUE_1_NAME})
    public void receiveMessageFromTopic1(String message) {
        System.out.println("Received topic 1 (" + BINDING_PATTERN_IMPORTANT + ") message: " + message);
    }

    @RabbitListener(queues = {TOPIC_QUEUE_2_NAME})
    public void receiveMessageFromTopic2(String message) {
        System.out.println("Received topic 2 (" + BINDING_PATTERN_ERROR + ") message: " + message);
    }

    /**
     * listener for MESSAGES_QUEUE. message will redirect to DLX_MESSAGES_EXCHANGE after any error or exception
     *
     * @param message
     * @throws BusinessException
     */
    @RabbitListener(queues = DLXParkingLotAmqpConfiguration.MESSAGES_QUEUE)
    public void receiveMessage(Message message) throws BusinessException {
        System.out.println("Received failed message, re-queueing: " + message.toString());
        System.out.println("Received failed message, re-queueing: " + message.getMessageProperties().getReceivedRoutingKey());
        throw new BusinessException();
    }

    /**
     * DLQ_MESSAGES_QUEUE will retry to send message again to original queue or exchange.
     * After maximum number of retry, message will redirect to parking lot queue or exchange
     *
     * @param message
     */
    @RabbitListener(queues = DLXParkingLotAmqpConfiguration.DLQ_MESSAGES_QUEUE)
    public void processFailedMessages(Message message) {
        Integer retryCount = (Integer) message.getMessageProperties().getHeaders().get(HEADER_X_RETRIES_COUNT);
        if (retryCount == null) {
            retryCount = 1;
        }
        if (retryCount > MAX_RETRIES_COUNT) {
            System.out.println("Sending message to the parking lot queue");
            this.template.send(DLXParkingLotAmqpConfiguration.EXCHANGE_PARKING_LOT,
                    message.getMessageProperties().getReceivedRoutingKey(), message);
            return;
        }
        System.out.println("################# Retrying message for the time: " + retryCount + " #####################");
        message.getMessageProperties().getHeaders().put(HEADER_X_RETRIES_COUNT, ++retryCount);
        this.template.send(DLXParkingLotAmqpConfiguration.MESSAGES_EXCHANGE, message.getMessageProperties().getReceivedRoutingKey(), message);
    }

    /**
     * parking lot queue listener
     * After maximum number of retry by DLQ, message will redirect to parking lot queue or exchange
     *
     * @param failedMessage
     */
    @RabbitListener(queues = DLXParkingLotAmqpConfiguration.QUEUE_PARKING_LOT)
    public void processParkingLotQueue(Message failedMessage) {
        System.out.println("Received message in parking lot queue" + failedMessage.toString());
        // Save to DB or send a notification.
    }

}
