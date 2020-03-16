package com.example.service.impl;

import com.example.model.Message;
import com.example.model.PullMessageResult;
import com.example.util.FileUtils;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import java.io.File;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class FileQueueService extends AbstractConcurrentCacheableQueueService {

    private static final String QUEUE_DIR_NAME = "queue";
    private static final String CACHE_DIR_NAME = "cache";

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
    protected Cache<String, PullMessageResult> readCache(String queueUrl) {
        return restoreCache(queueUrl);
    }

    @Override
    protected void writeCache(String queueUrl, Cache<String, PullMessageResult> messageCache) {
        File cacheFile = getCacheFile(queueUrl);
        FileUtils.writeData(cacheFile, new ConcurrentHashMap<>(messageCache.asMap()));
    }

    @Override
    protected Cache<String, PullMessageResult> buildCache(String queueUrl) {
        return CacheBuilder.newBuilder().build();
    }

    private File getQueueFile(String queueUrl) {
        return new File(storagePath + File.separator + QUEUE_DIR_NAME + File.separator + queueUrl);
    }

    private File getCacheFile(String queueUrl) {
        return new File(storagePath + File.separator + CACHE_DIR_NAME + File.separator + queueUrl);
    }

    private void initializeStorage(String storagePath) {
        File storageDir = new File(storagePath);
        if (!storageDir.exists() || !storageDir.isDirectory()) {
            createStorageDirectories(storageDir);
        } else if (storageDir.exists() && storageDir.isDirectory()) {
            restoreAllHiddenMessages();
        }
    }

    private void createStorageDirectories(File mainStorageDir) {
        mainStorageDir.mkdir();
        File queueDir = new File(storagePath + File.separator + QUEUE_DIR_NAME);
        File cacheDir = new File(storagePath + File.separator + CACHE_DIR_NAME);
        queueDir.mkdir();
        cacheDir.mkdir();
    }

    private void restoreAllHiddenMessages() {
        File cacheDir = new File(storagePath + File.separator + CACHE_DIR_NAME);
        if (cacheDir.exists() && cacheDir.isDirectory()) {
            String[] queueUrls = cacheDir.list();
            if (queueUrls != null) {
                for (String queueUrl : queueUrls) {
                    restoreCache(queueUrl);
                }
            }
        }
    }

    private Cache<String, PullMessageResult> restoreCache(String queueUrl) {
        File cacheFile = getCacheFile(queueUrl);
        ConcurrentMap<String, PullMessageResult> cacheData = FileUtils.readData(cacheFile);
        if (cacheData != null) {
            cacheData.values()
                    .parallelStream()
                    .filter(this::isInvisibleTimeExpired)
                    .forEach(pullResult -> {
                        restoreMessage(pullResult.getMessage(), queueUrl);
                        cacheData.remove(pullResult.getReceiptHandle());
                    });
            Cache<String, PullMessageResult> hiddenMessageCache = buildCache(queueUrl);
            hiddenMessageCache.putAll(cacheData);
            writeCache(queueUrl, hiddenMessageCache);
            return hiddenMessageCache;
        }
        return null;
    }

    private boolean isInvisibleTimeExpired(PullMessageResult pullMessageResult) {
        Instant invisibleExpirationDate = pullMessageResult.getReceiptDate().plus(visibilityTimeout);
        return Instant.now().isAfter(invisibleExpirationDate);
    }
}
