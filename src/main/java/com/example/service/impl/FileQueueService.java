package com.example.service.impl;

import com.example.model.CreateQueueResult;
import com.example.model.Message;
import com.example.model.PushMessageResult;
import com.example.service.QueueService;

public class FileQueueService implements QueueService {
    //
    // Task 3: Implement me if you have time.
    //



    @Override
    public CreateQueueResult createNewQueue() {
        return null;
    }

    @Override
    public void deleteQueue(String queueUrl) {

    }

    @Override
    public PushMessageResult push(String queueUrl, String messageBody) {
        return null;
    }

    @Override
    public Message pull(String queueUrl) {
        return null;
    }

    @Override
    public void delete(String queueUrl, String receiptHandle) {

    }
}
