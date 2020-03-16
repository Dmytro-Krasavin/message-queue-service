package com.example.service;

import com.example.model.CreateQueueResult;
import com.example.model.PullMessageResult;
import com.example.model.PushMessageResult;

public interface QueueService {

    /**
     * Creates a new queue.
     *
     * @return CreateQueueResult with generated queue URL
     */
    CreateQueueResult createNewQueue();

    /**
     * Deletes the queue specified by the queue URL if it is present.
     *
     * @param queueUrl The URL of the queue.
     */
    void deleteQueue(String queueUrl);

    /**
     * Pushes the specified message to the specified queue.
     *
     * @param queueUrl    The URL of the queue.
     * @param messageBody The message to send.
     * @return PushMessageResult with generated message ID
     * or {@code null} if the queue with this url does not exists.
     */
    PushMessageResult push(String queueUrl, String messageBody);

    /**
     * Retrieves a single message from the specified queue.
     * After retrieving the message becomes invisible for a certain time
     * and another consumers can not receive it. If the consumer that received the message
     * does not delete it, message automatically restores in queue after timeout expiration.
     *
     * @param queueUrl The URL of the queue.
     * @return PullMessageResult with single message and generated receiptHandle that can be used for deleting
     * or {@code null} if the queue with this url does not exists or it is empty.
     */
    PullMessageResult pull(String queueUrl);

    /**
     * Deletes the specified message from the specified queue.
     *
     * @param queueUrl      The URL of the queue
     * @param receiptHandle The receipt handle that was received using {@link #pull}
     */
    void delete(String queueUrl, String receiptHandle);

}
