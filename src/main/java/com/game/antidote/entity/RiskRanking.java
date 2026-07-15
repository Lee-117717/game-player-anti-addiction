package com.game.antidote.entity;

import lombok.Data;

/**
 * 高风险玩家排行榜实体 - 对应SQL数据集: ds_risk_ranking
 * 用于Top10排行榜表格: 未成年且有风险标签的玩家按在线时长排序
 */
@Data
public class RiskRanking {
    /** 玩家ID */
    private String playerId;
    /** 活跃天数 */
    private Integer activeDays;
    /** 累计在线小时数 */
    private Double totalHours;
    /** 日均在线分钟数 */
    private Integer avgDailyMin;
    /** 累计充值金额(元) */
    private Double totalRecharge;
    /** 最高风险等级 (1-3) */
    private Integer maxRiskLevel;
    /** 涉及游戏数量 */
    private Integer gameCount;
}
