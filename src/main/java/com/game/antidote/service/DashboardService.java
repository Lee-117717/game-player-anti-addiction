package com.game.antidote.service;

import java.util.Map;

/**
 * 防沉迷监控大屏数据服务接口
 * 每个方法对应一个大屏图表的完整数据 (已转换为ECharts可消费格式)
 */
public interface DashboardService {

    /** 数据集1: 当日核心KPI指标 */
    Map<String, Object> getKpiToday();

    /** 数据集2: 在线时长分布 (南丁格尔玫瑰图) */
    Map<String, Object> getDurationDist();

    /** 数据集3: 7天风险标签趋势 (堆叠柱状+折线双轴图) */
    Map<String, Object> getRiskTrend();

    /** 数据集4: 付费分析 (分组柱状+折线双轴图) */
    Map<String, Object> getPaymentAnalysis();

    /** 数据集5: 24小时时段登录统计 (堆叠面积图) */
    Map<String, Object> getPeriodStats();

    /** 数据集6: 高风险玩家Top10排行榜 */
    Map<String, Object> getRiskRanking();

    /** 数据集7: 违规玩家明细 */
    Map<String, Object> getViolationDetail();

    /** 数据集8: 未成年夜间违规明细 */
    Map<String, Object> getMinorNightDetail();

    /** 数据集9: 指定玩家明细 (TOP10下钻) */
    Map<String, Object> getPlayerDetail(String playerId);
}
