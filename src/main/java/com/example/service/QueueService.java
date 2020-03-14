package com.example.service;

import com.example.model.CreateQueueResult;
import com.example.model.Message;
import com.example.model.PushMessageResult;

public interface QueueService {

    //
    // Task 1: Define me.
    //
    // This interface should include the following methods.  You should choose appropriate
    // signatures for these methods that prioritise simplicity of implementation for the range of
    // intended implementations (in-memory, file, and SQS).  You may include additional methods if
    // you choose.
    //
    // - push
    //   pushes a message onto a queue.
    // - pull
    //   retrieves a single message from a queue.
    // - delete
    //   deletes a message from the queue that was received by pull().
    //

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
     * @return PushMessageResult with generated message ID.
     */
    PushMessageResult push(String queueUrl, String messageBody);

    /**
     * Retrieves a single message from the specified queue.
     * After retrieving the message becomes invisible for a certain time
     * and another consumers can not receive it. If the consumer that received the message
     * does not delete it, after timeout expiration message automatically restores in queue.
     *
     * @param queueUrl The URL of the queue.
     * @return Message with generated receiptHandle that can be used for deleting.
     */
    Message pull(String queueUrl);

    /**
     * Deletes the specified message from the specified queue.
     *
     * @param queueUrl      The URL of the queue
     * @param receiptHandle The receipt handle that was received using {@link #pull}
     */
    void delete(String queueUrl, String receiptHandle);

}
