package com.example.model;

import java.io.Serializable;
import java.util.Objects;

public class Message implements Serializable {

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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Message message = (Message) o;
        return Objects.equals(body, message.body) &&
                Objects.equals(messageId, message.messageId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(body, messageId);
    }
}
