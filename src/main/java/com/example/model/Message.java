package com.example.model;

public class Message {

    private final String body;
    private final String messageId;

    public Message(String body, String messageId) {
        this.body = body;
        this.messageId = messageId;
    }

    public String getBody() {
        return body;
    }

    public String getMessageId() {
        return messageId;
    }
}
