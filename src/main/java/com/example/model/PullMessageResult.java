package com.example.model;

import java.time.Instant;

public class PullMessageResult {

    private final Message message;
    private final String receiptHandle;
    private final Instant receiptDate;

    public PullMessageResult(Message message, String receiptHandle, Instant receiptDate) {
        this.message = message;
        this.receiptHandle = receiptHandle;
        this.receiptDate = receiptDate;
    }

    public Message getMessage() {
        return message;
    }

    public String getReceiptHandle() {
        return receiptHandle;
    }

    public Instant getReceiptDate() {
        return receiptDate;
    }
}
