package com.example.service.impl;

import com.example.model.Message;
import com.example.model.PullMessageResult;
import com.google.common.cache.Cache;

import java.io.*;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class FileQueueService extends AbstractConcurrentCacheableQueueService {

    private static final String QUEUE_DIR_NAME = "queue";
    private static final String CACHE_BACKUP_DIR_NAME = "cache_backup";

    private final String storagePath;

    public FileQueueService(Duration visibilityTimeout, String storagePath) {
        super(visibilityTimeout);
        this.storagePath = storagePath;
        initializeStorage(storagePath);
    }

    @Override
    @SuppressWarnings("unchecked")
    protected BlockingDeque<Message> readQueue(String queueUrl) {
        File queueFile = getQueueFile(queueUrl);
        try (FileInputStream fileInputStream = new FileInputStream(queueFile);
             ObjectInputStream objectInputStream = new ObjectInputStream(fileInputStream)) {
            return (BlockingDeque<Message>) objectInputStream.readObject();
        } catch (IOException | ClassNotFoundException e) {
            return null;
        }
    }

    @Override
    protected void writeQueue(String queueUrl, BlockingDeque<Message> messageQueue) {
        File queueFile = getQueueFile(queueUrl);
        try (FileOutputStream fileOutputStream = new FileOutputStream(queueFile);
             ObjectOutputStream objectOutputStream = new ObjectOutputStream(fileOutputStream)) {
            objectOutputStream.writeObject(messageQueue);
        } catch (IOException e) {
        }
    }

    @Override
    protected void removeQueue(String queueUrl) {
        File queueFile = getQueueFile(queueUrl);
        if (queueFile.exists()) {
            queueFile.delete();
        }
    }

    @SuppressWarnings("unchecked")
    protected Cache<String, PullMessageResult> readCache(String queueUrl) {
        File cacheFile = getCacheBackupFile(queueUrl);
        try (FileInputStream fileInputStream = new FileInputStream(cacheFile);
             ObjectInputStream objectInputStream = new ObjectInputStream(fileInputStream)) {

            ConcurrentMap<String, PullMessageResult> initialCacheData = (ConcurrentMap<String, PullMessageResult>) objectInputStream.readObject();
            if (initialCacheData != null) {
                Cache<String, PullMessageResult> messageCache = super.buildMessageCache(queueUrl);
                messageCache.putAll(initialCacheData);
                return messageCache;
            }
            return null;
        } catch (IOException | ClassNotFoundException e) {
            return null;
        }
    }

    protected void writeCache(String queueUrl, Cache<String, PullMessageResult> messageCache) {
        File cacheFile = getCacheBackupFile(queueUrl);
        try (FileOutputStream fileOutputStream = new FileOutputStream(cacheFile);
             ObjectOutputStream objectOutputStream = new ObjectOutputStream(fileOutputStream)) {
            ConcurrentMap<String, PullMessageResult> cacheData = new ConcurrentHashMap<>(messageCache.asMap());
            objectOutputStream.writeObject(cacheData);
        } catch (IOException e) {
        }
    }

    public static void main(String[] args) {
        FileQueueService queueService = new FileQueueService(Duration.ofMillis(1), "test/");
        String queueUrl = "8dab0607-d7bf-4cef-91db-ce4582a90e01";
//        BlockingDeque<Message> messages = queueService.readQueue(queueUrl);
//        messages.addFirst(new Message("message-2", "2"));
//        queueService.writeQueue(queueUrl, messages);
//        System.out.println(messages.toString());

//        queueService.writeCache(queueUrl, queueService.buildMessageCache(queueUrl));
//        Cache<String, PullMessageResult> cache = queueService.readCache(queueUrl);
//        System.out.println(cache);
    }

    private File getQueueFile(String queueUrl) {
        return new File(storagePath + File.separator + QUEUE_DIR_NAME + File.separator + queueUrl);
    }

    private File getCacheBackupFile(String queueUrl) {
        return new File(storagePath + File.separator + CACHE_BACKUP_DIR_NAME + File.separator + queueUrl);
    }

    private void initializeStorage(String storagePath) {
        File mainStorageDir = new File(storagePath);
        if (mainStorageDir.exists() && mainStorageDir.isDirectory()) {

            File cacheDir = new File(storagePath + File.separator + CACHE_BACKUP_DIR_NAME);
            if (cacheDir.exists() && cacheDir.isDirectory()) {
                String[] fileNames = cacheDir.list();
                if (fileNames != null) {
                    for (String fileName : fileNames) {
                        restoreAllExpiredMessage(fileName);
                    }
                }
            }
        } else if (!mainStorageDir.exists() || !mainStorageDir.isDirectory()) {
            mainStorageDir.mkdir();
            File queueDir = new File(storagePath + File.separator + QUEUE_DIR_NAME);
            File cacheDir = new File(storagePath + File.separator + CACHE_BACKUP_DIR_NAME);
            queueDir.mkdir();
            cacheDir.mkdir();
        }
    }

    private void restoreAllExpiredMessage(String queueUrl) {
        Cache<String, PullMessageResult> hiddenMessageCache = readCache(queueUrl);
        if (hiddenMessageCache != null) {
            hiddenMessageCache.asMap().values()
                    .parallelStream()
                    .filter(this::isInvisibleTimeExpired)
                    .forEach(pullResult -> {
                        restoreMessage(pullResult.getMessage(), queueUrl);
                        hiddenMessageCache.invalidate(pullResult.getReceiptHandle());
                    });
        }
    }

    private boolean isInvisibleTimeExpired(PullMessageResult pullMessageResult) {
        Instant invisibleExpirationDate = pullMessageResult.getReceiptDate().plus(visibilityTimeout);
        return Instant.now().isAfter(invisibleExpirationDate);
    }
}
