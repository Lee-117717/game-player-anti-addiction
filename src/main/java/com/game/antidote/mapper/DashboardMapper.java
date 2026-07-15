package com.game.antidote.mapper;

import com.game.antidote.entity.*;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

import java.util.List;

/**
 * 防沉迷大屏数据 Mapper 接口
 * 封装全部8个数据集的Doris查询SQL
 * 使用 #{latestDt} 参数化日期，避免硬编码
 */
public interface DashboardMapper {

    /**
     * 获取表中最新日期
     * 用于所有SQL的参数绑定，确保查询最新数据分区
     */
    @Select("SELECT MAX(dt) FROM dwd_game_player_behavior")
    String selectMaxDt();

    /**
     * 数据集1: 当日核心KPI指标 (6个指标卡)
     */
    KpiToday selectKpiToday(@Param("latestDt") String latestDt);

    /**
     * 数据集2: 在线时长分布 (南丁格尔玫瑰图)
     */
    List<DurationDist> selectDurationDist(@Param("latestDt") String latestDt);

    /**
     * 数据集3: 7天风险标签趋势 (堆叠柱状+折线双轴图)
     */
    List<RiskTrend> selectRiskTrend(@Param("latestDt") String latestDt);

    /**
     * 数据集4: 付费分析 (分组柱状+折线双轴图)
     */
    List<PaymentAnalysis> selectPaymentAnalysis(@Param("latestDt") String latestDt);

    /**
     * 数据集5: 24小时时段登录统计 (堆叠面积图)
     */
    List<PeriodStats> selectPeriodStats(@Param("latestDt") String latestDt);

    /**
     * 数据集6: 高风险玩家Top10排行榜
     */
    List<RiskRanking> selectRiskRanking(@Param("latestDt") String latestDt);

    /**
     * 数据集7: 违规玩家明细 (自动滚动表格)
     */
    List<ViolationDetail> selectViolationDetail(@Param("latestDt") String latestDt);

    /**
     * 数据集8: 未成年夜间违规明细 (下钻弹窗)
     */
    List<MinorNightDetail> selectMinorNightDetail(@Param("latestDt") String latestDt);

    /**
     * 数据集9: 指定玩家的全部明细记录 (TOP10下钻弹窗)
     */
    List<MinorNightDetail> selectPlayerDetail(@Param("playerId") String playerId, @Param("latestDt") String latestDt);
}
