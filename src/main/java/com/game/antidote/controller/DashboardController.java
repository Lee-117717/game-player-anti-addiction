package com.game.antidote.controller;

import com.game.antidote.common.Result;
import com.game.antidote.service.DashboardService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

/**
 * 游戏防沉迷监控大屏 REST API 控制器
 *
 * 提供8组数据接口 + 健康检查，统一使用 Result<T> 封装返回。
 * 前端 Vue3 通过 axios 异步请求获取 JSON 数据。
 *
 * API基础路径: /api
 */
@RestController
@RequestMapping("/api")
public class DashboardController {

    @Autowired
    private DashboardService dashboardService;

    /** 统一异常处理 */
    private Result<Map<String, Object>> safeQuery(String name, java.util.function.Supplier<Map<String, Object>> query) {
        try {
            Map<String, Object> data = query.get();
            return Result.ok(data);
        } catch (Exception e) {
            return Result.fail("[" + name + "] 数据查询失败: " + e.getMessage());
        }
    }

    // ================================================================
    // 接口1: 当日核心KPI指标 (6个指标卡)
    // GET /api/kpi/today
    // ================================================================
    @GetMapping("/kpi/today")
    public Result<Map<String, Object>> getKpiToday() {
        return safeQuery("KPI指标", () -> dashboardService.getKpiToday());
    }

    // ================================================================
    // 接口2: 在线时长分布 (南丁格尔玫瑰图)
    // GET /api/duration/dist
    // ================================================================
    @GetMapping("/duration/dist")
    public Result<Map<String, Object>> getDurationDist() {
        return safeQuery("时长分布", () -> dashboardService.getDurationDist());
    }

    // ================================================================
    // 接口3: 7天风险趋势 (堆叠柱状+折线双轴组合图)
    // GET /api/risk/trend
    // ================================================================
    @GetMapping("/risk/trend")
    public Result<Map<String, Object>> getRiskTrend() {
        return safeQuery("风险趋势", () -> dashboardService.getRiskTrend());
    }

    // ================================================================
    // 接口4: 付费分析 (分组柱状+折线双轴图)
    // GET /api/payment/analysis
    // ================================================================
    @GetMapping("/payment/analysis")
    public Result<Map<String, Object>> getPaymentAnalysis() {
        return safeQuery("付费分析", () -> dashboardService.getPaymentAnalysis());
    }

    // ================================================================
    // 接口5: 24小时时段登录统计 (堆叠面积图)
    // GET /api/period/stats
    // ================================================================
    @GetMapping("/period/stats")
    public Result<Map<String, Object>> getPeriodStats() {
        return safeQuery("时段统计", () -> dashboardService.getPeriodStats());
    }

    // ================================================================
    // 接口6: 高风险玩家Top10排行榜
    // GET /api/risk/ranking
    // ================================================================
    @GetMapping("/risk/ranking")
    public Result<Map<String, Object>> getRiskRanking() {
        return safeQuery("风险排行", () -> dashboardService.getRiskRanking());
    }

    // ================================================================
    // 接口7: 违规玩家明细 (自动滚动表格)
    // GET /api/violation/detail
    // ================================================================
    @GetMapping("/violation/detail")
    public Result<Map<String, Object>> getViolationDetail() {
        return safeQuery("违规明细", () -> dashboardService.getViolationDetail());
    }

    // ================================================================
    // 接口8: 未成年夜间违规明细 (下钻弹窗)
    // GET /api/minor/night-detail
    // ================================================================
    @GetMapping("/minor/night-detail")
    public Result<Map<String, Object>> getMinorNightDetail() {
        return safeQuery("夜间违规明细", () -> dashboardService.getMinorNightDetail());
    }

    /** 健康检查 */
    @GetMapping("/health")
    public Result<String> health() {
        return Result.ok("UP", "anti-addiction-dashboard");
    }

    // ================================================================
    // 接口9: 指定玩家明细 (TOP10下钻弹窗)
    // GET /api/player/detail?playerId=M73318
    // ================================================================
    @GetMapping("/player/detail")
    public Result<Map<String, Object>> getPlayerDetail(@RequestParam String playerId) {
        return safeQuery("玩家明细", () -> dashboardService.getPlayerDetail(playerId));
    }
}
