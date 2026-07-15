package com.game.antidote.entity;

import lombok.Data;

/**
 * 付费分析实体 - 对应SQL数据集: ds_payment_analysis
 * 用于分组柱状图+折线双轴图: 6档充值区间 × 成人/未成年分组
 */
@Data
public class PaymentAnalysis {
    /** 账号类型: 'adult'(成年) / 'minor'(未成年) */
    private String accountType;
    /** 充值区间: '未付费' / '1~30元' / '30~100元' / '100~300元' / '300~1000元' / '1000元以上' */
    private String rechargeRange;
    /** 去重玩家数 */
    private Integer playerCount;
    /** 总充值金额(元) */
    private Double totalAmount;
    /** 平均充值金额(元) */
    private Double avgAmount;
}
