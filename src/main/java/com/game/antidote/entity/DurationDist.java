package com.game.antidote.entity;

import lombok.Data;

/**
 * 在线时长分布实体 - 对应SQL数据集: ds_duration_dist
 * 用于南丁格尔玫瑰图: 6个时长区间(player_count, record_count, pct)
 */
@Data
public class DurationDist {
    /** 时长区间标签: '<30分钟' / '30~60分钟' / '1~2小时' / '2~3小时' / '3~4小时' / '4小时以上(超限)' */
    private String durationRange;
    /** 去重玩家数 */
    private Integer playerCount;
    /** 行为记录数(不去重) */
    private Integer recordCount;
    /** 占比百分比 */
    private Double pct;
}
