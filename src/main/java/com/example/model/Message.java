package com.example.model;

public class Message {

    private final String body;
    private final String messageId;

    private String receiptHandle;

    public Message(String body, String messageId) {
        this.body = body;
        this.messageId = messageId;
    }

    public Message(String body, String messageId, String receiptHandle) {
        this.body = body;
        this.messageId = messageId;
        this.receiptHandle = receiptHandle;
    }

    public String getBody() {
        return body;
    }

    public String getMessageId() {
        return messageId;
    }

    public String getReceiptHandle() {
        return receiptHandle;
    }

    public void setReceiptHandle(String receiptHandle) {
        this.receiptHandle = receiptHandle;
    }
}
