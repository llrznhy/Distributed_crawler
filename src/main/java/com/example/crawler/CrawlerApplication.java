package com.example.crawler;

import com.example.crawler.config.CrawlerConfig;
import com.example.crawler.core.*;
import com.example.crawler.distributed.downloader.DistributedDownloader;
import com.example.crawler.distributed.downloader.DownloadTask;
import com.example.crawler.distributed.manager.DistributedUrlManager;
import com.example.crawler.distributed.parser.DistributedParser;
import com.example.crawler.distributed.queue.KafkaMessageQueue;
import com.example.crawler.distributed.queue.MessageQueue;
import com.example.crawler.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

/**
 * 爬虫主应用类
 * 整合所有基础要求的实现：
 * - 系统关键模块基于分布式思想进行实现
 * - 在单机上实现多进程并发爬取
 * - 实现基础的分布式架构
 *
 * 实现的基础功能：
 * 1. URL管理和分发 (a,b,c,d)
 * 2. DNS解析 (e) - 待实现
 * 3. 数据爬取 (f)
 * 4. 数据解析 (g)
 * 5. 数据存储 (h)
 * 6. 数据查询 (i)
 * 7. 代理支持 (j) - 待实现
 * 8. 监控功能 (k)
 */
public class CrawlerApplication {
    private static final Logger logger = LoggerFactory.getLogger(CrawlerApplication.class);

    // 原有字段
    private final DistributedDownloader distributedDownloader; ;  // 替换原来的 Downloader
    //    private final Parser parser;
    private final DistributedParser distributedParser; // 替换原来的 Parser
    private final DataStore dataStore;
    private final CrawlerMonitor monitor;
    private final int threadCount;
    private final ExecutorService executorService;
    private volatile boolean isRunning = true;

    // 分布式相关字段
    private final String nodeId;
    private final MessageQueue messageQueue;
    private final DistributedUrlManager urlManager; // 替换原来的 UrlManager

    public CrawlerApplication(String nodeId, int threadCount) {  // 修改构造函数
        this.nodeId = nodeId;
        this.threadCount = threadCount;

        // 初始化消息队列
        this.messageQueue = new KafkaMessageQueue(
                CrawlerConfig.getString("kafka.bootstrap.servers", "localhost:9092"),
                CrawlerConfig.getString("kafka.group.id", "crawler-group")
        );

        // 使用分布式URL管理器
        this.urlManager = new DistributedUrlManager(
                nodeId,
                CrawlerConfig.getMaxDepth(),
                messageQueue,
                CrawlerConfig.getInt("crawler.batch.size", 100),
                CrawlerConfig.getLong("crawler.distribution.interval", 5000)
        );

        // 初始化其他组件，保持不变
//        this.downloader = new Downloader(
//                CrawlerConfig.getUserAgent(),
//                CrawlerConfig.getInt("crawler.connect.timeout", 5000),
//                CrawlerConfig.getInt("crawler.read.timeout", 10000)
//        );
        this.distributedDownloader = new DistributedDownloader(nodeId, messageQueue);
        this.distributedParser = new DistributedParser(nodeId, messageQueue);

        this.dataStore = new DataStore(
                CrawlerConfig.getString("storage.path", "./crawler_data"),
                CrawlerConfig.getString("storage.format", "json")
        );
        this.monitor = new CrawlerMonitor();
        this.executorService = Executors.newFixedThreadPool(threadCount);
    }

    public void start(String seedUrl, int maxPages) {
        logger.info("Starting crawler with seed URL: {} using {} threads", seedUrl, threadCount);
        urlManager.addUrl(seedUrl, 0);

        // 启动监控线程
        ScheduledExecutorService monitorService = Executors.newSingleThreadScheduledExecutor();
        monitorService.scheduleAtFixedRate(
                () -> monitor.logStatistics(),
                1, 1, TimeUnit.MINUTES
        );

        // 创建并启动工作线程
        List<Future<?>> futures = new ArrayList<>();
        for (int i = 0; i < threadCount; i++) {
            futures.add(executorService.submit(() -> crawl(maxPages)));
        }

        // 等待所有线程完成
        try {
            for (Future<?> future : futures) {
                future.get();
            }
        } catch (Exception e) {
            logger.error("Error waiting for crawler threads", e);
        } finally {
            shutdown(monitorService);
        }
    }

    private void crawl(int maxPages) {
        monitor.incrementActiveThreads();
        try {
            while (isRunning && monitor.getProcessedPages() < maxPages) {
                CrawlRequest request = urlManager.getNext();
                if (request == null) {
                    Thread.sleep(1000);  // 如果没有URL，等待一段时间
                    continue;
                }

                processUrl(request);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            monitor.decrementActiveThreads();
        }
    }

    private void processUrl(CrawlRequest request) {
        try {
            logger.info("Thread {} creating download task for URL: {} (Depth: {})",
                    Thread.currentThread().getId(), request.getUrl(), request.getDepth());

            // 创建下载任务
            DownloadTask task = new DownloadTask(
                    request.getUrl(),
                    request.getDepth(),
                    nodeId
            );

            // 发送到下载任务队列
            messageQueue.send("download_tasks", task);

            // 记录处理状态
            monitor.recordPageProcessed(request.getUrl(), true);

            // 爬取延迟
            Thread.sleep(CrawlerConfig.getCrawlDelay());

        } catch (Exception e) {
            logger.error("Error creating download task for URL {}: {}",
                    request.getUrl(), e.getMessage());
            monitor.recordPageProcessed(request.getUrl(), false);
        }
    }

    private void shutdown(ExecutorService... services) {
        isRunning = false;
        for (ExecutorService service : services) {
            service.shutdown();
            try {
                if (!service.awaitTermination(5, TimeUnit.SECONDS)) {
                    service.shutdownNow();
                }
            } catch (InterruptedException e) {
                service.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
        executorService.shutdown();
    }

    public static void main(String[] args) {
        // 从配置获取节点ID
        String nodeId = CrawlerConfig.getString("node.id", "node-1");

        // 创建爬虫实例，添加节点ID参数
        CrawlerApplication crawler = new CrawlerApplication(nodeId, 4);

        // 爬取维基百科的 Java 页面
        String seedUrl = "https://zh.wikipedia.org/wiki/Python";
        int maxPages = 5;  // 先爬取少量页面测试

        // 开始爬取
        crawler.start(seedUrl, maxPages);
    }
}