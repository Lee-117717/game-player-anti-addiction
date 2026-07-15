package com.game.antidote.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;

/**
 * 定时任务配置
 * 定期刷新 Doris 数据缓存，保证大屏数据实时性
 */
@Configuration
@EnableScheduling
public class ScheduledTaskConfig {

    private static final Logger log = LoggerFactory.getLogger(ScheduledTaskConfig.class);

    /**
     * 每30秒输出心跳日志 (数据由前端主动轮询拉取)
     * 大屏数据刷新由前端 setInterval 控制，后端无需主动推送
     */
    @Scheduled(fixedRate = 30000)
    public void heartbeat() {
        log.debug("大屏数据就绪, 等待前端轮询...");
    }
}
