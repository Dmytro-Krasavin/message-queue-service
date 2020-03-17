package com.example.model;

import java.io.Serializable;
import java.time.Instant;
import java.util.Objects;

public class PullMessageResult implements Serializable {

    private static final long serialVersionUID = 1L;

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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PullMessageResult that = (PullMessageResult) o;
        return Objects.equals(message, that.message) &&
                Objects.equals(receiptHandle, that.receiptHandle) &&
                Objects.equals(receiptDate, that.receiptDate);
    }

    @Override
    public int hashCode() {
        return Objects.hash(message, receiptHandle, receiptDate);
    }
}
