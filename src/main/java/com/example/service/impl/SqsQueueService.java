package com.example.service.impl;

import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;
import com.amazonaws.services.sqs.model.SendMessageResult;
import com.example.model.CreateQueueResult;
import com.example.model.Message;
import com.example.model.PullMessageResult;
import com.example.model.PushMessageResult;
import com.example.service.QueueService;

import java.time.Instant;
import java.util.UUID;

public class SqsQueueService implements QueueService {

    private final AmazonSQSClient sqsClient;

    public SqsQueueService(AmazonSQSClient sqsClient) {
        this.sqsClient = sqsClient;
    }

    @Override
    public CreateQueueResult createNewQueue() {
        String queueName = UUID.randomUUID().toString();
        com.amazonaws.services.sqs.model.CreateQueueResult queue = sqsClient.createQueue(queueName);
        return new CreateQueueResult(queue.getQueueUrl());
    }

    @Override
    public void deleteQueue(String queueUrl) {
        sqsClient.deleteQueue(queueUrl);
    }

    @Override
    public PushMessageResult push(String queueUrl, String messageBody) {
        SendMessageResult result = sqsClient.sendMessage(queueUrl, messageBody);
        return new PushMessageResult(result.getMessageId());
    }

    @Override
    public PullMessageResult pull(String queueUrl) {
        ReceiveMessageResult result = sqsClient.receiveMessage(queueUrl);
        Instant receiptDate = Instant.now();
        return result.getMessages().stream()
                .map(message -> {
                    Message messageModel = new Message(message.getBody(), message.getMessageId());
                    return new PullMessageResult(messageModel, message.getReceiptHandle(), receiptDate);
                })
                .findFirst()
                .orElse(null);
    }

    @Override
    public void delete(String queueUrl, String receiptHandle) {
        sqsClient.deleteMessage(queueUrl, receiptHandle);
    }
}
