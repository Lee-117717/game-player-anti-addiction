package com.game.antidote.entity;

import lombok.Data;

/**
 * 时段登录统计实体 - 对应SQL数据集: ds_period_stats
 * 用于24小时堆叠面积图: adult_active + minor_active 堆叠
 */
@Data
public class PeriodStats {
    /** 登录小时 (0-23) */
    private Integer hourOfDay;
    /** 成年玩家去重活跃数 */
    private Integer adultActive;
    /** 未成年玩家去重活跃数 */
    private Integer minorActive;
    /** 总活跃数 */
    private Integer totalActive;
    /** 是否夜间: '夜间'(22-7) / '非夜间'(8-21) */
    private String nightFlag;
}
