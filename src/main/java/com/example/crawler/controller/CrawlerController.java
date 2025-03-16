package com.example.crawler.controller;

import com.example.crawler.service.CrawlerService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.*;

@Controller
public class CrawlerController {

    @Autowired
    private CrawlerService crawlerService;

    // 首页展示爬虫状态
    @GetMapping("/")
    public String index(Model model) {
        model.addAttribute("status", crawlerService.getStatus());
        model.addAttribute("processedPages", crawlerService.getProcessedPages());
        return "index";
    }

    // 启动爬虫
    @PostMapping("/start")
    public String startCrawler(@RequestParam String seedUrl, @RequestParam int maxPages) {
        crawlerService.startCrawler(seedUrl, maxPages);
        return "redirect:/";
    }



    // 停止爬虫
    @PostMapping("/stop")
    public String stopCrawler() {
        crawlerService.stopCrawler();
        return "redirect:/";
    }
}

