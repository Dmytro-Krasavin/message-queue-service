package com.example.model;

public class PushMessageResult {

    private final String messageId;

    public PushMessageResult(String messageId) {
        this.messageId = messageId;
    }

    public String getMessageId() {
        return messageId;
    }
}
