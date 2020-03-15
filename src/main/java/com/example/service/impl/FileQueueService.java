package com.example.service.impl;

import com.example.model.Message;
import com.example.model.PullMessageResult;
import com.google.common.cache.Cache;

import java.io.*;
import java.time.Duration;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.ConcurrentMap;

public class FileQueueService extends AbstractConcurrentCacheableQueueService {

    private static final String QUEUE_DIR_NAME = "queue";
    private static final String CACHE_DIR_NAME = "cache";

    private final String storagePath;

    public FileQueueService(Duration visibilityTimeout, String storagePath) {
        super(visibilityTimeout);
        this.storagePath = storagePath;
//        initializeStorage(storagePath);
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
    protected void writeCache(String queueUrl, Cache<String, PullMessageResult> messageCache) {
        File cacheFile = getCacheFile(queueUrl);
        try (FileOutputStream fileOutputStream = new FileOutputStream(cacheFile);
             ObjectOutputStream objectOutputStream = new ObjectOutputStream(fileOutputStream)) {
            objectOutputStream.writeObject(messageCache.asMap());
        } catch (IOException e) {
        }
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
    @SuppressWarnings("unchecked")
    protected Cache<String, PullMessageResult> readCache(String queueUrl) {
        File cacheFile = getCacheFile(queueUrl);
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

    @Override
    protected void removeQueue(String queueUrl) {
        File queueFile = getQueueFile(queueUrl);
        if (queueFile.exists()) {
            queueFile.delete();
        }
    }

    @Override
    protected void removeCache(String queueUrl) {
        File cacheFile = getCacheFile(queueUrl);
        if (cacheFile.exists()) {
            cacheFile.delete();
        }
    }

    private File getQueueFile(String queueUrl) {
        return new File(storagePath + File.separator + QUEUE_DIR_NAME + File.separator + queueUrl);
    }

    private File getCacheFile(String queueUrl) {
        return new File(storagePath + File.separator + CACHE_DIR_NAME + File.separator + queueUrl);
    }

    private void initializeStorage(String storagePath) throws IOException {
//        File file = new File(storagePath);
//        if (file.exists() && file.isDirectory()) {
//            try (Stream<Path> fileStream = Files.walk(file.toPath())) {
//                Set<Message> messages = fileStream.filter(Files::isRegularFile)
//                        .map(Path::toFile)
//                        .map(this::readQueueFile)
//                        .flatMap(Collection::parallelStream)
//                        .collect(Collectors.toSet());
////                result.forEach(System.out::println);
//
//            }
//        } else if (!file.exists() || !file.isDirectory()) {
//            file.mkdir();
//            File queueDir = new File(storagePath + File.separator + QUEUE_DIR_NAME);
//            File cacheDir = new File(storagePath + File.separator + CACHE_DIR_NAME);
//            File cacheConfigDir = new File(storagePath + File.separator + CACHE_CONFIG_DIR_NAME);
//            queueDir.mkdir();
//            cacheDir.mkdir();
//            cacheConfigDir.mkdir();
//        }
    }
}
