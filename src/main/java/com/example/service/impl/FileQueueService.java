package com.example.service.impl;

import com.example.model.Message;
import com.example.model.PullMessageResult;
import com.example.util.FileUtils;
import com.google.common.cache.Cache;

import java.io.File;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class FileQueueService extends AbstractConcurrentCacheableQueueService {

    private static final String QUEUE_DIR_NAME = "queue";
    private static final String CACHE_BACKUP_DIR_NAME = "cache-backup";

    private final String storagePath;

    public FileQueueService(Duration visibilityTimeout, String storagePath) {
        super(visibilityTimeout);
        this.storagePath = storagePath;
        initializeStorage(storagePath);
    }

    @Override
    protected BlockingDeque<Message> readQueue(String queueUrl) {
        File queueFile = getQueueFile(queueUrl);
        return FileUtils.readData(queueFile);
    }

    @Override
    protected void writeQueue(String queueUrl, BlockingDeque<Message> messageQueue) {
        File queueFile = getQueueFile(queueUrl);
        FileUtils.writeData(queueFile, messageQueue);
    }

    @Override
    protected void writeCache(String queueUrl, Cache<String, PullMessageResult> messageCache) {
        super.writeCache(queueUrl, messageCache);
        File cacheFile = getCacheBackupFile(queueUrl);
        FileUtils.writeData(cacheFile, new ConcurrentHashMap<>(messageCache.asMap()));
    }

    private File getQueueFile(String queueUrl) {
        return new File(storagePath + File.separator + QUEUE_DIR_NAME + File.separator + queueUrl);
    }

    private File getCacheBackupFile(String queueUrl) {
        return new File(storagePath + File.separator + CACHE_BACKUP_DIR_NAME + File.separator + queueUrl);
    }

    private void initializeStorage(String storagePath) {
        File storageDir = new File(storagePath);
        if (!storageDir.exists() || !storageDir.isDirectory()) {
            createStorageDirectories(storageDir);
        } else if (storageDir.exists() && storageDir.isDirectory()) {
            restoreCache();
        }
    }

    private void createStorageDirectories(File mainStorageDir) {
        mainStorageDir.mkdir();
        File queueDir = new File(storagePath + File.separator + QUEUE_DIR_NAME);
        File cacheDir = new File(storagePath + File.separator + CACHE_BACKUP_DIR_NAME);
        queueDir.mkdir();
        cacheDir.mkdir();
    }

    private void restoreCache() {
        File cacheDir = new File(storagePath + File.separator + CACHE_BACKUP_DIR_NAME);
        if (cacheDir.exists() && cacheDir.isDirectory()) {
            String[] queueUrls = cacheDir.list();
            if (queueUrls != null) {
                for (String queueUrl : queueUrls) {
                    restoreAllExpiredMessages(queueUrl);
                }
            }
        }
    }

    private void restoreAllExpiredMessages(String queueUrl) {
        File cacheBackupFile = getCacheBackupFile(queueUrl);
        ConcurrentMap<String, PullMessageResult> cacheData = FileUtils.readData(cacheBackupFile);
        if (cacheData != null) {
            cacheData.values()
                    .parallelStream()
                    .filter(this::isInvisibleTimeExpired)
                    .forEach(pullResult -> {
                        restoreMessage(pullResult.getMessage(), queueUrl);
                        cacheData.remove(pullResult.getReceiptHandle());
                    });
            Cache<String, PullMessageResult> hiddenMessageCache = buildMessageCache(queueUrl, cacheData);
            writeCache(queueUrl, hiddenMessageCache);
        }
    }

    private boolean isInvisibleTimeExpired(PullMessageResult pullMessageResult) {
        Instant invisibleExpirationDate = pullMessageResult.getReceiptDate().plus(visibilityTimeout);
        return Instant.now().isAfter(invisibleExpirationDate);
    }
}
