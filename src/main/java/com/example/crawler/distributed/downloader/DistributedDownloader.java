//数据爬取的分布式实现

package com.example.crawler.distributed.downloader;
import com.example.crawler.core.Downloader;
import com.example.crawler.config.CrawlerConfig;
import com.example.crawler.distributed.queue.MessageQueue;
import com.example.crawler.model.PageContent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.*;

public class DistributedDownloader extends Downloader {
    private static final Logger logger = LoggerFactory.getLogger(DistributedDownloader.class);

    private final String nodeId;
    private final MessageQueue messageQueue;
    private final ExecutorService downloadExecutor;
    private final int threadPoolSize;
    private volatile boolean isRunning = true;

    public DistributedDownloader(String nodeId, MessageQueue messageQueue) {
        super(CrawlerConfig.getUserAgent(),
                CrawlerConfig.getInt("crawler.connect.timeout", 5000),
                CrawlerConfig.getInt("crawler.read.timeout", 10000));

        this.nodeId = nodeId;
        this.messageQueue = messageQueue;
        this.threadPoolSize = CrawlerConfig.getInt("downloader.thread.pool.size", 10);
        this.downloadExecutor = Executors.newFixedThreadPool(threadPoolSize);


        initialize();
    }

    private void initialize() {
        // 订阅下载任务
        messageQueue.subscribe("download_tasks", message -> {
            if (message.getContent() instanceof DownloadTask) {
                DownloadTask task = (DownloadTask) message.getContent();
                processDownloadTask(task);
            }
        });

        logger.info("Distributed downloader initialized with {} threads", threadPoolSize);
    }

    private void processDownloadTask(DownloadTask task) {
        if (!isRunning) return;

        downloadExecutor.submit(() -> {
            long startTime = System.currentTimeMillis();
            try {
                PageContent content = super.download(task.getUrl());
                DownloadResult result = new DownloadResult.Builder()
                        .url(task.getUrl())
                        .content(content)
                        .success(content != null)
                        .downloadTime(System.currentTimeMillis() - startTime)
                        .nodeId(nodeId)
                        .build();

                // 发送结果到解析队列
                messageQueue.send("parse_tasks", result);

                logger.debug("Downloaded URL: {} in {}ms", task.getUrl(),
                        result.getDownloadTime());

            } catch (Exception e) {
                DownloadResult result = new DownloadResult.Builder()
                        .url(task.getUrl())
                        .success(false)
                        .error(e.getMessage())
                        .downloadTime(System.currentTimeMillis() - startTime)
                        .nodeId(nodeId)
                        .build();

                messageQueue.send("download_failures", result);

                logger.error("Failed to download URL: {} - {}", task.getUrl(),
                        e.getMessage());
            }
        });
    }

    @Override
    public void close() {
        isRunning = false;
        if (downloadExecutor != null) {
            downloadExecutor.shutdown();
            try {
                if (!downloadExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                    downloadExecutor.shutdownNow();
                }
            } catch (InterruptedException e) {
                downloadExecutor.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
    }
}