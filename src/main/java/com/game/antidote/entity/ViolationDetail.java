package com.game.antidote.entity;

import lombok.Data;

/**
 * 违规玩家明细实体 - 对应SQL数据集: ds_violation_detail
 * 用于自动滚动明细表格: risk_label > 0 的玩家详细行为记录
 */
@Data
public class ViolationDetail {
    /** 统计日期 */
    private String statDate;
    /** 玩家ID */
    private String playerId;
    /** 游戏ID */
    private String gameId;
    /** 登录时间 */
    private String loginTime;
    /** 下线时间 */
    private String logoutTime;
    /** 在线时长(分钟) */
    private Integer onlineMin;
    /** 登录IP */
    private String loginIp;
    /** 设备类型: android / ios / pc */
    private String deviceType;
    /** 游戏区域 */
    private String gameRegion;
    /** 风险类型(中文): '超时预警' / '夜间违规' / '重度沉迷' */
    private String riskType;
    /** 充值金额(元) */
    private Double rechargeAmount;
    /** 对局次数 */
    private Integer matchCount;
}
