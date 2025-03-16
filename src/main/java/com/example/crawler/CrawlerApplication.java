package com.example.crawler;

import com.example.crawler.config.CrawlerConfig;
import com.example.crawler.core.*;
import com.example.crawler.distributed.downloader.DistributedDownloader;
import com.example.crawler.distributed.downloader.DownloadResult;
import com.example.crawler.distributed.downloader.DownloadTask;
import com.example.crawler.distributed.manager.DistributedUrlManager;
import com.example.crawler.distributed.parser.DistributedParser;
import com.example.crawler.distributed.queue.KafkaMessageQueue;
import com.example.crawler.distributed.queue.Message;
import com.example.crawler.distributed.queue.MessageHandler;
import com.example.crawler.distributed.queue.MessageQueue;
import com.example.crawler.model.*;
import com.example.crawler.service.CrawlerService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.select.Elements;
import org.jsoup.nodes.Element;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import io.prometheus.client.Counter;
import io.prometheus.client.Gauge;
import io.prometheus.client.exporter.HTTPServer;
import org.springframework.scheduling.annotation.EnableAsync;

import java.io.IOException;

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

@SpringBootApplication
@EnableAsync
public class CrawlerApplication {
    private static final Logger logger = LoggerFactory.getLogger(CrawlerApplication.class);

    // 原有字段
    private final DistributedDownloader distributedDownloader; ;  // 替换原来的 Downloader
    //    private final Parser parser;
    private final DistributedParser distributedParser; // 替换原来的 Parser
    private final DataStore dataStore;
    private final CrawlerMonitor monitor;
    private final int threadCount;
    private ExecutorService executorService;
    private volatile boolean isRunning = true;

    // 分布式相关字段
    private final String nodeId;
    private final MessageQueue messageQueue;
    private final DistributedUrlManager urlManager; // 替换原来的 UrlManager



//    private static final Counter pagesProcessed = Counter.build()
//            .name("crawler_pages_processed_total")
//            .help("Total number of pages processed")
//            .register();
//
//    private static final Counter errorCount = Counter.build()
//            .name("crawler_errors_total")
//            .help("Total number of errors")
//            .register();
//
//    private static final Gauge queueSize = Gauge.build()
//            .name("crawler_queue_size")
//            .help("Current size of the URL queue")
//            .register();
//
//    private HTTPServer prometheusServer;


    public CrawlerApplication(@Value("${crawler.thread.count}") int threadCount,
                              @Value("${node.id}") String nodeId)  {  // 修改构造函数
        this.nodeId = nodeId;
        this.threadCount = threadCount;

//        try {
//            this.prometheusServer = new HTTPServer(9091); // 启动 Prometheus 服务器
//        } catch (IOException e) {
//            logger.error("Failed to start Prometheus HTTPServer: {}", e.getMessage());
//        }

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

//    public void start(String seedUrl, int maxPages) {
//        logger.info("Starting crawler with seed URL: {} using {} threads", seedUrl, threadCount);
//        urlManager.addUrl(seedUrl, 0);
//
//        // 启动监控线程
//        ScheduledExecutorService monitorService = Executors.newSingleThreadScheduledExecutor();
//        monitorService.scheduleAtFixedRate(
//                () -> monitor.logStatistics(),
//                1, 1, TimeUnit.MINUTES
//        );
//
//        // 创建并启动工作线程
//        List<Future<?>> futures = new ArrayList<>();
//        for (int i = 0; i < threadCount; i++) {
//            futures.add(executorService.submit(() -> crawl(maxPages)));
//        }
//
//        // 等待所有线程完成
//        try {
//            for (Future<?> future : futures) {
//                future.get();
//            }
//        } catch (Exception e) {
//            logger.error("Error waiting for crawler threads", e);
//        } finally {
//            shutdown(monitorService);
//        }
//    }
    public void start(String seedUrl, int maxPages) {
        if (executorService == null || executorService.isShutdown()) {
            executorService = Executors.newFixedThreadPool(threadCount);  // 重新初始化线程池
            isRunning = true;
        }

        logger.info("Starting crawler with seed URL: {} using {} threads", seedUrl, threadCount);
        urlManager.addUrl(seedUrl, 0);

        // 启动监控线程
        ScheduledExecutorService monitorService = Executors.newSingleThreadScheduledExecutor();
        monitorService.scheduleAtFixedRate(
                () -> monitor.logStatistics(),
                1, 1, TimeUnit.MINUTES
        );
//        monitorService.scheduleAtFixedRate(() -> {
//            queueSize.labels(nodeId).set(urlManager.getQueueSize());
//            logger.info("Updated queue size: {}", urlManager.getQueueSize());
//        }, 1, 10, TimeUnit.SECONDS);


        // 创建并启动工作线程
        List<Future<List<CrawlResult>>> futures = new ArrayList<>();
        for (int i = 0; i < threadCount; i++) {
            futures.add((Future<List<CrawlResult>>) executorService.submit(() -> crawl(maxPages)));
        }
        

        // ✅ 存储爬取的结果
        // ✅ 处理爬取的结果
        try {
            for (Future<List<CrawlResult>> future : futures) {
                List<CrawlResult> results = future.get();
                for (CrawlResult result : results) {
                    if (result != null) {
                        // 生成 JSON 文件名
                        String filename = "wiki_" + result.getUrl().hashCode() + "_" + System.currentTimeMillis() + ".json";

                        // ✅ 修改 store 方法调用，传入 filename
                        dataStore.store(result, filename);
                    }
                }
            }
        } catch (Exception e) {
            logger.error("Error waiting for crawl results: {}", e.getMessage());
        } finally {
            shutdown(monitorService);
        }

    }


//    private void crawl(int maxPages) {
//        monitor.incrementActiveThreads();
//        try {
//            while (isRunning && monitor.getProcessedPages() < maxPages) {
//                CrawlRequest request = urlManager.getNext();
//                if (request == null) {
//                    Thread.sleep(1000);  // 如果没有URL，等待一段时间
//                    continue;
//                }
//
//                processUrl(request);
//            }
//        } catch (InterruptedException e) {
//            Thread.currentThread().interrupt();
//        } finally {
//            monitor.decrementActiveThreads();
//        }
//    }

//    private void crawl(int maxPages) {
//        monitor.incrementActiveThreads();
//        logger.info("Total pages processed: {}", monitor.getProcessedPages());
//
//
//
//        try {
//            while (isRunning && monitor.getProcessedPages() < maxPages) {
//                CrawlRequest request = urlManager.getNext();
//                if (request == null) {
//                    Thread.sleep(1000);
//                    continue;
//                }
//
//                CrawlResult result = processUrl(request);
//                if (result != null) {
//                    dataStore.store(result);  // ✅ 确保数据立即存储
//                    logger.info("Stored JSON result for URL: {}", result.getUrl());
//                } else {
//                    logger.warn("CrawlResult is null for URL: {}", request.getUrl());
//                }
//            }
//        } catch (InterruptedException e) {
//            Thread.currentThread().interrupt();
//        } finally {
//            monitor.decrementActiveThreads();
//        }
//    }

    private void crawl(int maxPages) {
        monitor.incrementActiveThreads();


        try {
            while (isRunning) {
                if (monitor.getProcessedPages() >= maxPages) {
                    break;  // ✅ 确保只爬取 maxPages 个页面
                }

                CrawlRequest request = urlManager.getNext();
                if (request == null) {
                    Thread.sleep(1000);
                    continue;
                }

                CrawlResult result = processUrl(request, maxPages);
                if (result != null) {
                    // 生成文件名
                    String filename = "wiki_" + request.getUrl().hashCode() + "_" + System.currentTimeMillis() + ".json";

                    // 传递两个参数
                    dataStore.store(result, filename);

                    monitor.recordPageProcessed(request.getUrl(), true);
                    logger.info("Stored JSON for URL: {} as {}", request.getUrl(), filename);
                } else {
                    logger.warn("CrawlResult is null for URL: {}", request.getUrl());
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            monitor.decrementActiveThreads();
        }
        logger.info("Processed pages count: {}", monitor.getProcessedPages());

    }




//    private void processUrl(CrawlRequest request) {
//        try {
//            logger.info("Thread {} creating download task for URL: {} (Depth: {})",
//                    Thread.currentThread().getId(), request.getUrl(), request.getDepth());
//
//            // 创建下载任务
//            DownloadTask task = new DownloadTask(
//                    request.getUrl(),
//                    request.getDepth(),
//                    nodeId
//            );
//
//            // 发送到下载任务队列
//            messageQueue.send("download_tasks", task);
//
//            // 记录处理状态
//            monitor.recordPageProcessed(request.getUrl(), true);
//
//            // 爬取延迟
//            Thread.sleep(CrawlerConfig.getCrawlDelay());
//
//        } catch (Exception e) {
//            logger.error("Error creating download task for URL {}: {}",
//                    request.getUrl(), e.getMessage());
//            monitor.recordPageProcessed(request.getUrl(), false);
//        }
//    }



//    private CrawlResult processUrl(CrawlRequest request) {
//
//        try {
//            logger.info("Thread {} processing URL: {} (Depth: {})",
//                    Thread.currentThread().getId(), request.getUrl(), request.getDepth());
//
//            // 使用 Jsoup 获取网页内容
//            Document doc = Jsoup.connect(request.getUrl())
//                    .timeout(10_000)  // 超时时间 10 秒
//                    .get();
//            String content = doc.html();  // 获取完整 HTML
//
//            // **提取页面的标题（可选）**
//            String title = doc.title();
//
//            // **提取所有超链接（可选）**
//            Elements links = doc.select("a[href]");
//            List<String> extractedUrls = new ArrayList<>();
//            for (Element link : links) {
//                extractedUrls.add(link.absUrl("href"));
//            }
//
//            // **创建爬取结果**
//            CrawlResult result = new CrawlResult(request.getUrl(), content, 200, title, extractedUrls);
//
//            // **存储 JSON 文件**
//            dataStore.store(result);
//            logger.info("Successfully stored JSON for URL: {}", request.getUrl());
//
//            // **返回爬取结果**
//            return result;
//        } catch (Exception e) {
//            logger.error("Error processing URL {}: {}", request.getUrl(), e.getMessage());
//        }
//        return null;
//    }

    private CrawlResult processUrl(CrawlRequest request, int maxPages) {
        try {
            logger.info("Thread {} processing URL: {} (Depth: {})",
                    Thread.currentThread().getId(), request.getUrl(), request.getDepth());


            if (monitor.getProcessedPages() >= maxPages) {
                return null;
            }


            // ✅ 1. 发送下载任务到 Kafka
            DownloadTask task = new DownloadTask(request.getUrl(), request.getDepth(), nodeId);
            messageQueue.send("download_tasks", task);
            logger.info("Download task sent to Kafka for URL: {}", request.getUrl());

            // ✅ 2. 模拟下载：等待 Kafka 处理下载任务
            Thread.sleep(2000); // 等待 2 秒，模拟分布式下载
            logger.info("Waiting for download to complete...");

            // ✅ 3. 解析网页内容
            Document doc = Jsoup.connect(request.getUrl())
                    .timeout(10_000)
                    .get();
            String content = doc.html();
            String title = doc.title();

            // ✅ 4. 提取超链接（只保留 Wikipedia 相关的）
            Elements links = doc.select("a[href]");
            List<String> extractedUrls = new ArrayList<>();
            for (Element link : links) {
                String absoluteUrl = link.absUrl("href");

                // 过滤非 Wikipedia 相关的 URL
                if (!absoluteUrl.contains("wikipedia.org")) {
                    continue;
                }

                // 只在新的 URL 时打印日志
                if (!urlManager.hasUrl(absoluteUrl)) {
                    logger.info("Added new URL to queue: {}", absoluteUrl);
                }
                extractedUrls.add(absoluteUrl);
                urlManager.addUrl(absoluteUrl, request.getDepth() + 1);
            }

            // ✅ 5. 生成 JSON 文件名（使用 URL 哈希值）
            String filename = "wiki_" + request.getUrl().hashCode() + "_" +
                    System.currentTimeMillis() + ".json";

            // ✅ 6. 存储爬取结果
            CrawlResult result = new CrawlResult(request.getUrl(), content, 200, title, extractedUrls);
            dataStore.store(result, filename);
            monitor.recordPageProcessed(request.getUrl(), true);

            logger.info("Stored JSON for URL: {} as {}", request.getUrl(), filename);

//            增加 URL 过滤
            for (Element link : links) {
                String absoluteUrl = link.absUrl("href");

                // 只爬取 Wikipedia 页面
                if (!absoluteUrl.contains("wikipedia.org")) {
                    continue;
                }

                // 检查 URL 是否已经存在
                if (urlManager.hasUrl(absoluteUrl)) {
                    logger.info("Skipping duplicate URL: {}", absoluteUrl);
                    continue;
                }

                // 记录 URL，并加入爬取任务
                extractedUrls.add(absoluteUrl);
                urlManager.addUrl(absoluteUrl, request.getDepth() + 1);
                logger.info("Added new URL to queue: {}", absoluteUrl);
            }

            return result;
        } catch (Exception e) {
            logger.error("Error processing URL {}: {}", request.getUrl(), e.getMessage());
            monitor.recordPageProcessed(request.getUrl(), false);
        }
        return null;
    }

//    private CrawlResult processUrl(CrawlRequest request, int maxPages) {
//        try {
//            logger.info("Thread {} processing URL: {} (Depth: {})",
//                    Thread.currentThread().getId(), request.getUrl(), request.getDepth());
//
//            // ✅ 1. 先检查是否超过 maxPages 限制
//            if (monitor.getProcessedPages() >= maxPages) {
//                return null;
//            }
//
//            // ✅ 2. 发送下载任务到 Kafka
//            DownloadTask task = new DownloadTask(request.getUrl(), request.getDepth(), nodeId);
//            messageQueue.send("download_tasks", task);
//            logger.info("Download task sent to Kafka for URL: {}", request.getUrl());
//
//            // ✅ 3. 轮询 Kafka 消息队列，等待下载完成
//            DownloadResult downloadResult = waitForDownloadResult(request.getUrl());
//            if (downloadResult == null || downloadResult.getContent().isEmpty()) {
//                logger.warn("Download failed or empty content for URL: {}", request.getUrl());
//                monitor.recordPageProcessed(request.getUrl(), false);
//                return null;
//            }
//
//            // ✅ 4. 解析网页内容
//            Document doc = Jsoup.parse(downloadResult.getContent().getHtml());
//            String content = doc.html();
//            String title = doc.title();
//
//            // ✅ 5. 提取超链接（只保留 Wikipedia 相关的）
//            Elements links = doc.select("a[href]");
//            List<String> extractedUrls = new ArrayList<>();
//
//            for (Element link : links) {
//                String absoluteUrl = link.absUrl("href");
//
//                // 过滤非 Wikipedia 相关的 URL
//                if (!absoluteUrl.contains("wikipedia.org")) {
//                    continue;
//                }
//
//                // 检查 URL 是否已经存在
//                if (urlManager.hasUrl(absoluteUrl)) {
//                    logger.info("Skipping duplicate URL: {}", absoluteUrl);
//                    continue;
//                }
//
//                extractedUrls.add(absoluteUrl);
//                urlManager.addUrl(absoluteUrl, request.getDepth() + 1);
//                logger.info("Added new URL to queue: {}", absoluteUrl);
//            }
//
//            // ✅ 6. 生成 JSON 文件名
//            String filename = "wiki_" + request.getUrl().hashCode() + "_" +
//                    System.currentTimeMillis() + ".json";
//
//            // ✅ 7. 存储爬取结果
//            CrawlResult result = new CrawlResult(request.getUrl(), content, 200, title, extractedUrls);
//            dataStore.store(result, filename);
//            monitor.recordPageProcessed(request.getUrl(), true);
//
//            logger.info("Stored JSON for URL: {} as {}", request.getUrl(), filename);
//
//            return result;
//        } catch (Exception e) {
//            logger.error("Error processing URL {}: {}", request.getUrl(), e.getMessage());
//            monitor.recordPageProcessed(request.getUrl(), false);
//        }
//        return null;
//    }

    /**
     * 轮询 Kafka 消息队列，等待下载任务完成
     */
    private DownloadResult waitForDownloadResult(String url) {
        int retries = 10; // 最多尝试 10 次
        for (int i = 0; i < retries; i++) {
            // ✅ 直接消费 Kafka 消息队列
            DownloadResult result = messageQueue.poll("download_results", url);
            if (result != null) {
                logger.info("Download result received for URL: {}", url);
                return result;
            }
            try {
                Thread.sleep(1000); // 每次轮询间隔 1 秒
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
        logger.warn("Timeout waiting for download result: {}", url);
        return null; // 超时未获取到下载结果
    }




//    public void shutdown(ExecutorService... services) {
//        isRunning = false;
//        for (ExecutorService service : services) {
//            service.shutdown();
//            try {
//                if (!service.awaitTermination(5, TimeUnit.SECONDS)) {
//                    service.shutdownNow();
//                }
//            } catch (InterruptedException e) {
//                service.shutdownNow();
//                Thread.currentThread().interrupt();
//            }
//        }
//        executorService.shutdown();
//    }
    public void shutdown(ExecutorService... services) {
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
        if (executorService != null) {
            executorService.shutdown();
        }
        executorService = null; // 标记 executorService 为 null，便于下次重新启动
    }





    public static void main(String[] args) {
//        // 从配置获取节点ID
//        String nodeId = CrawlerConfig.getString("node.id", "node-1");
//
//        // 创建爬虫实例，添加节点ID参数
//        CrawlerApplication crawler = new CrawlerApplication(4, nodeId);
//
//        // 爬取维基百科的 Java 页面
//        String seedUrl = "https://zh.wikipedia.org/wiki/Java";
//        int maxPages = 5;  // 先爬取少量页面测试
//
//        // 开始爬取
//        crawler.start(seedUrl, maxPages);

        SpringApplication.run(CrawlerApplication.class, args);
    }
}
