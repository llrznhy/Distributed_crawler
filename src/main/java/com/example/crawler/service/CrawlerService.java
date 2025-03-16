package com.example.crawler.service;

import com.example.crawler.CrawlerApplication;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

@Service
public class CrawlerService {

    private boolean running = false;
    private int processedPages = 0;

    @Autowired
    private CrawlerApplication crawlerApplication;

    // 启动爬虫
    @Async
    public void startCrawler(String seedUrl, int maxPages) {
        running = true;
        processedPages = 0;

        System.out.println("爬虫启动，种子URL: " + seedUrl + "，最大页数: " + maxPages);

        // 调用爬虫核心逻辑
        crawlerApplication.start(seedUrl, maxPages);

        running = false;
    }

    // 停止爬虫
    public void stopCrawler() {
        running = false;
        crawlerApplication.shutdown(); // 停止爬虫
        System.out.println("爬虫已停止");
    }

    // 返回爬虫状态
    public boolean getStatus() {
        return running;
    }

    // 获取已处理页面数
    public int getProcessedPages() {
        return processedPages;
    }

    // 模拟增加已处理页面数
    public void incrementProcessedPages() {
        processedPages++;
    }
}
