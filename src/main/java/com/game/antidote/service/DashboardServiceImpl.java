package com.game.antidote.service;

import com.game.antidote.entity.*;
import com.game.antidote.mapper.DashboardMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.*;

/**
 * 防沉迷大屏数据服务实现
 *
 * 核心职责: 将Doris原始查询结果(List<Entity>)转换为ECharts可直接消费的JSON结构
 * 转换逻辑:
 * - KPI:   单行Entity → 键值对Map
 * - 玫瑰图: List<DurationDist> → {names, values, pcts} 三个平行数组
 * - 趋势图: List<RiskTrend> → {categories, dau, level1, level2, level3, riskRate, minorRiskRate}
 * - 付费图: List<PaymentAnalysis> → {categories, adult[], minor[]} 按账号类型拆分系列
 * - 面积图: List<PeriodStats> → {hours, adultActive[], minorActive[]} 补全缺失的24小时
 * - 表格:  直接返回原始List
 */
@Service
public class DashboardServiceImpl implements DashboardService {

    @Autowired
    private DashboardMapper dashboardMapper;

    /**
     * 获取最新数据日期
     * 所有SQL均以此日期为参数，确保查询最新分区
     */
    private String getLatestDt() {
        return dashboardMapper.selectMaxDt();
    }

    // ================================================================
    // 数据集1: KPI指标卡
    // ================================================================
    @Override
    public Map<String, Object> getKpiToday() {
        String latestDt = getLatestDt();
        KpiToday kpi = dashboardMapper.selectKpiToday(latestDt);

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("dt", latestDt);
        result.put("totalActive",    kpi.getTotalActive());     // 当日活跃
        result.put("minorActive",    kpi.getMinorActive());     // 未成年活跃
        result.put("atRiskCount",    kpi.getAtRiskCount());     // 风险玩家
        result.put("severeCount",    kpi.getSevereCount());     // 重度沉迷
        result.put("totalRecharge",  kpi.getTotalRecharge());   // 总充值(元)
        result.put("avgOnlineMin",   kpi.getAvgOnlineMin());    // 平均在线(分钟)
        return result;
    }

    // ================================================================
    // 数据集2: 在线时长分布 → 南丁格尔玫瑰图
    // ECharts pie/rose 需要: [{name, value}, ...]
    // ================================================================
    @Override
    public Map<String, Object> getDurationDist() {
        String latestDt = getLatestDt();
        List<DurationDist> list = dashboardMapper.selectDurationDist(latestDt);

        List<String> names = new ArrayList<>();
        List<Integer> values = new ArrayList<>();
        List<Double> pcts = new ArrayList<>();

        for (DurationDist item : list) {
            names.add(item.getDurationRange());
            values.add(item.getPlayerCount());
            pcts.add(item.getPct());
        }

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("names", names);       // 维度: ['<30分钟', '30~60分钟', ...]
        result.put("values", values);     // 数值: [1200, 800, ...]
        result.put("pcts", pcts);         // 占比: [20.5, 13.6, ...]
        return result;
    }

    // ================================================================
    // 数据集3: 7天风险趋势 → 堆叠柱状+折线双轴组合图
    // 左轴(柱状): dau, level1/2/3_count; 右轴(折线): risk_rate_pct, minor_risk_rate_pct
    // ================================================================
    @Override
    public Map<String, Object> getRiskTrend() {
        String latestDt = getLatestDt();
        List<RiskTrend> list = dashboardMapper.selectRiskTrend(latestDt);

        List<String> categories = new ArrayList<>();    // X轴日期
        List<Integer> dauList = new ArrayList<>();      // DAU
        List<Integer> l1 = new ArrayList<>();           // 一级预警
        List<Integer> l2 = new ArrayList<>();           // 二级违规
        List<Integer> l3 = new ArrayList<>();           // 重度沉迷
        List<Double> riskRate = new ArrayList<>();      // 总风险率
        List<Double> minorRiskRate = new ArrayList<>(); // 未成年风险率

        for (RiskTrend item : list) {
            categories.add(item.getStatDate());
            dauList.add(item.getDau());
            l1.add(item.getLevel1Count());
            l2.add(item.getLevel2Count());
            l3.add(item.getLevel3Count());
            riskRate.add(item.getRiskRatePct());
            minorRiskRate.add(item.getMinorRiskRatePct());
        }

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("categories", categories);
        result.put("dau", dauList);
        result.put("level1", l1);
        result.put("level2", l2);
        result.put("level3", l3);
        result.put("riskRate", riskRate);
        result.put("minorRiskRate", minorRiskRate);
        return result;
    }

    // ================================================================
    // 数据集4: 付费分析 → 分组柱状+折线双轴图
    // SQL返回按 account_type + recharge_range 分组，需拆分为两个平行系列
    // ================================================================
    @Override
    public Map<String, Object> getPaymentAnalysis() {
        String latestDt = getLatestDt();
        List<PaymentAnalysis> list = dashboardMapper.selectPaymentAnalysis(latestDt);

        // 固定6档充值区间作为X轴顺序
        String[] allRanges = {"未付费", "1~30元", "30~100元", "100~300元", "300~1000元", "1000元以上"};

        Map<String, Integer> adultCount = new LinkedHashMap<>();
        Map<String, Double> adultAmount = new LinkedHashMap<>();
        Map<String, Integer> minorCount = new LinkedHashMap<>();
        Map<String, Double> minorAmount = new LinkedHashMap<>();

        for (String range : allRanges) {
            adultCount.put(range, 0);
            adultAmount.put(range, 0.0);
            minorCount.put(range, 0);
            minorAmount.put(range, 0.0);
        }

        for (PaymentAnalysis item : list) {
            if ("adult".equals(item.getAccountType())) {
                adultCount.put(item.getRechargeRange(), item.getPlayerCount());
                adultAmount.put(item.getRechargeRange(), item.getTotalAmount());
            } else {
                minorCount.put(item.getRechargeRange(), item.getPlayerCount());
                minorAmount.put(item.getRechargeRange(), item.getTotalAmount());
            }
        }

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("categories", allRanges);
        result.put("adultCount", new ArrayList<>(adultCount.values()));
        result.put("adultAmount", new ArrayList<>(adultAmount.values()));
        result.put("minorCount", new ArrayList<>(minorCount.values()));
        result.put("minorAmount", new ArrayList<>(minorAmount.values()));
        return result;
    }

    // ================================================================
    // 数据集5: 24小时时段统计 → 堆叠面积图
    // 关键: Doris只返回有数据的小时，需用0填充缺失的24小时
    // ================================================================
    @Override
    public Map<String, Object> getPeriodStats() {
        String latestDt = getLatestDt();
        List<PeriodStats> list = dashboardMapper.selectPeriodStats(latestDt);

        // 建立小时→数据映射
        Map<Integer, PeriodStats> dataMap = new HashMap<>();
        for (PeriodStats item : list) {
            dataMap.put(item.getHourOfDay(), item);
        }

        List<Integer> hours = new ArrayList<>();
        List<Integer> adultActive = new ArrayList<>();
        List<Integer> minorActive = new ArrayList<>();

        // 补全24小时 (0-23)
        for (int h = 0; h < 24; h++) {
            hours.add(h);
            PeriodStats item = dataMap.get(h);
            adultActive.add(item != null ? item.getAdultActive() : 0);
            minorActive.add(item != null ? item.getMinorActive() : 0);
        }

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("hours", hours);
        result.put("adultActive", adultActive);
        result.put("minorActive", minorActive);
        return result;
    }

    // ================================================================
    // 数据集6: 高风险玩家Top10 → 排行表格
    // ================================================================
    @Override
    public Map<String, Object> getRiskRanking() {
        String latestDt = getLatestDt();
        List<RiskRanking> list = dashboardMapper.selectRiskRanking(latestDt);

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("data", list);
        result.put("total", list.size());
        return result;
    }

    // ================================================================
    // 数据集7: 违规玩家明细 → 自动滚动表格
    // ================================================================
    @Override
    public Map<String, Object> getViolationDetail() {
        String latestDt = getLatestDt();
        List<ViolationDetail> list = dashboardMapper.selectViolationDetail(latestDt);

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("data", list);
        result.put("total", list.size());
        return result;
    }

    // ================================================================
    // 数据集8: 未成年夜间违规明细 → 下钻弹窗
    // ================================================================
    @Override
    public Map<String, Object> getMinorNightDetail() {
        String latestDt = getLatestDt();
        List<MinorNightDetail> list = dashboardMapper.selectMinorNightDetail(latestDt);

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("data", list);
        result.put("total", list.size());
        return result;
    }

    // ================================================================
    // 数据集9: 指定玩家明细 → TOP10下钻弹窗
    // ================================================================
    @Override
    public Map<String, Object> getPlayerDetail(String playerId) {
        String latestDt = getLatestDt();
        List<MinorNightDetail> list = dashboardMapper.selectPlayerDetail(playerId, latestDt);

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("data", list);
        result.put("total", list.size());
        return result;
    }
}
