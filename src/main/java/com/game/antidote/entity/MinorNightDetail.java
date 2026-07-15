package com.game.antidote.entity;

import lombok.Data;

/**
 * 未成年夜间违规明细实体 - 对应SQL数据集: ds_minor_night_detail
 * 用于下钻弹窗/子页面: is_minor=1 AND risk_label IN (2,3) 的详细记录
 */
@Data
public class MinorNightDetail {
    /** 日期 */
    private String dt;
    /** 玩家ID */
    private String playerId;
    /** 登录时间 */
    private String loginTime;
    /** 下线时间 */
    private String logoutTime;
    /** 在线时长(分钟) */
    private Integer onlineMin;
    /** 登录小时 (0-23) */
    private Integer loginHour;
    /** 设备类型 */
    private String deviceType;
    /** 游戏区域 */
    private String gameRegion;
    /** 游戏ID */
    private String gameId;
    /** 充值金额(元) */
    private Double rechargeAmount;
    /** 风险描述: '二级违规(仅夜间)' / '重度沉迷(夜间+超时)' */
    private String riskDesc;
}
