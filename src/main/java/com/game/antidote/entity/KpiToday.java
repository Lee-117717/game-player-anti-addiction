package com.game.antidote.entity;

import lombok.Data;

/**
 * 当日核心指标实体 - 对应SQL数据集: ds_kpi_today
 * 用于顶部6个KPI指标卡: 当日活跃、未成年活跃、风险玩家、重度沉迷、总充值、平均在线时长
 */
@Data
public class KpiToday {
    /** 当日活跃玩家总数 */
    private Integer totalActive;
    /** 未成年活跃玩家数 */
    private Integer minorActive;
    /** 风险玩家总数 (risk_label > 0) */
    private Integer atRiskCount;
    /** 重度沉迷玩家数 (risk_label = 3) */
    private Integer severeCount;
    /** 当日总充值金额(元) */
    private Double totalRecharge;
    /** 当日平均在线时长(分钟) */
    private Integer avgOnlineMin;
}
