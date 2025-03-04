package com.example.crawler.distributed.parser;

import com.example.crawler.core.Parser;
import com.example.crawler.distributed.queue.MessageQueue;
import com.example.crawler.model.CrawlResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.*;

public class DistributedParser extends Parser {
    private static final Logger logger = LoggerFactory.getLogger(DistributedParser.class);

    private final String nodeId;
    private final MessageQueue messageQueue;
    private final ExecutorService parseExecutor;
    private final int threadPoolSize;
    private volatile boolean isRunning = true;

    public DistributedParser(String nodeId, MessageQueue messageQueue) {
        super();
        this.nodeId = nodeId;
        this.messageQueue = messageQueue;
        this.threadPoolSize = 5; // 可配置
        this.parseExecutor = Executors.newFixedThreadPool(threadPoolSize);

        initialize();
    }

    private void initialize() {
        // 订阅解析任务
        messageQueue.subscribe("parse_tasks", message -> {
            if (message.getContent() instanceof ParseTask) {
                ParseTask task = (ParseTask) message.getContent();
                processParseTask(task);
            }
        });

        logger.info("Distributed parser initialized with {} threads", threadPoolSize);
    }

    private void processParseTask(ParseTask task) {
        if (!isRunning) return;

        parseExecutor.submit(() -> {
            long startTime = System.currentTimeMillis();
            try {
                CrawlResult result = super.parse(task.getContent());
                ParseResult parseResult = new ParseResult.Builder()
                        .result(result)
                        .sourceNodeId(nodeId)
                        .parseTime(System.currentTimeMillis() - startTime)
                        .success(result != null)
                        .build();

                // 发送解析结果到存储队列
                messageQueue.send("store_results", parseResult);

                // 如果解析成功，发送新的URL到管理器
                if (result != null && !result.getLinks().isEmpty()) {
                    messageQueue.send("new_urls", result.getLinks());
                }

                logger.debug("Parsed content from URL: {} in {}ms",
                        task.getContent().getUrl(), parseResult.getParseTime());

            } catch (Exception e) {
                ParseResult parseResult = new ParseResult.Builder()
                        .sourceNodeId(nodeId)
                        .parseTime(System.currentTimeMillis() - startTime)
                        .success(false)
                        .error(e.getMessage())
                        .build();

                messageQueue.send("parse_failures", parseResult);

                logger.error("Failed to parse content from URL: {} - {}",
                        task.getContent().getUrl(), e.getMessage());
            }
        });
    }

    public void close() {
        isRunning = false;
        if (parseExecutor != null) {
            parseExecutor.shutdown();
            try {
                if (!parseExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                    parseExecutor.shutdownNow();
                }
            } catch (InterruptedException e) {
                parseExecutor.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
    }
}