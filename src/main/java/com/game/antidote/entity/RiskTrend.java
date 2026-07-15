package com.game.antidote.entity;

import lombok.Data;

/**
 * 风险标签趋势实体 - 对应SQL数据集: ds_risk_trend
 * 用于堆叠柱状图+折线双轴组合图: 7天风险趋势
 */
@Data
public class RiskTrend {
    /** 统计日期 */
    private String statDate;
    /** 日活跃用户数 */
    private Integer dau;
    /** 一级预警人数 (risk_label=1, 超时>4h) */
    private Integer level1Count;
    /** 二级违规人数 (risk_label=2, 夜间登录) */
    private Integer level2Count;
    /** 重度沉迷人数 (risk_label=3, 超时+夜间) */
    private Integer level3Count;
    /** 总体风险率百分比 */
    private Double riskRatePct;
    /** 未成年风险率百分比 */
    private Double minorRiskRatePct;
}
