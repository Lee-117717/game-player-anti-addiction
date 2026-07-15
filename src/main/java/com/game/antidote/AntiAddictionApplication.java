package com.game.antidote;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * 游戏防沉迷监控大屏 - SpringBoot 启动入口
 *
 * 技术栈: SpringBoot 2.7.x + MyBatis-Plus 3.5.x + Druid 1.2.x + Apache Doris 2.0
 * 前端:  ECharts 5 (CDN加载) 单页大屏
 *
 * @author Game Anti-Addiction Team
 */
@SpringBootApplication
@MapperScan("com.game.antidote.mapper")
public class AntiAddictionApplication {

    public static void main(String[] args) {
        SpringApplication.run(AntiAddictionApplication.class, args);
        System.out.println("========================================");
        System.out.println("  游戏防沉迷监控大屏已启动");
        System.out.println("  API 地址: http://localhost:8081/api");
        System.out.println("  大屏页面: http://localhost:8081/big_screen.html");
        System.out.println("  Druid监控: http://localhost:8081/druid");
        System.out.println("========================================");
    }
}
