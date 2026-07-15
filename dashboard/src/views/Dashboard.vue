<template>
  <div class="dash-outer">
  <div class="dash-wrap" :style="dashStyle">
    <!-- ========== 导航栏 ========== -->
    <NavHeader />

    <!-- ========== KPI: 2行×3列 ========== -->
    <LayoutRow :cols="3">
      <KpiCard label="当日活跃玩家"   :value="kpi.totalActive"   unit="人" />
      <KpiCard label="未成年活跃玩家" :value="kpi.minorActive"   unit="人" />
      <KpiCard label="总充值金额"     :value="kpi.totalRecharge" unit="元" prefix="¥" />
    </LayoutRow>
    <LayoutRow :cols="3">
      <KpiCard label="风险玩家数"     :value="kpi.atRiskCount"   unit="人 (风险预警)" :warn="kpi.atRiskCount > 0" />
      <KpiCard label="重度沉迷玩家"   :value="kpi.severeCount"   unit="人 (需干预)"   :danger="kpi.severeCount > 0" />
      <KpiCard label="平均在线时长"   :value="kpi.avgOnlineMin"  unit="分钟" />
    </LayoutRow>

    <!-- ========== 图表行1: 玫瑰图 + 风险趋势 ========== -->
    <LayoutRow :cols="2">
      <div class="panel echart-wrap">
        <div class="panel-title">在线时长分布</div>
        <BaseEchart :option="roseOption" height="440px" />
      </div>
      <div class="panel echart-wrap">
        <div class="panel-title">风险标签趋势（近7天）</div>
        <BaseEchart :option="trendOption" height="440px" />
      </div>
    </LayoutRow>

    <!-- ========== 图表行2: 面积图 + 付费分析 ========== -->
    <LayoutRow :cols="2">
      <div class="panel echart-wrap">
        <div class="panel-title">24小时登录活跃分布</div>
        <BaseEchart :option="periodOption" height="440px" />
      </div>
      <div class="panel echart-wrap">
        <div class="panel-title">账号类型-付费区间流量桑基图</div>
        <BaseEchart :option="paymentOption" height="480px" />
      </div>
    </LayoutRow>

    <!-- ========== 表格行: TOP10 + 滚动明细 (比图表矮 ~100px) ========== -->
    <LayoutRow :cols="2" class="table-row">
      <BaseTable
        title="高风险玩家 TOP10（点击查看玩家明细）"
        :columns="rankCols" :data="ranking"
        :maxRows="10" :rowClickable="true" :riskField="'maxRiskLevel'"
        @rowClick="onRankClick"
      />
      <BaseTable
        title="违规玩家实时明细（自动滚动）"
        :columns="violationCols" :data="violations"
        :scroll="true" :maxRows="12" :riskField="'riskType'"
      />
    </LayoutRow>

    <!-- ========== 弹窗 ========== -->
    <DetailModal :visible="modalVisible" :data="modalData" @close="modalVisible = false" />

    <!-- ========== 底部 ========== -->
    <div class="footer-bar">
      <span>数据源: Apache Doris 2.0 | 游戏防沉迷系统</span>
      <span>刷新: KPI 30s | 图表 60s | 表格 120s</span>
    </div>
  </div>
  </div>
</template>

<script setup>
import { ref, reactive, computed, onMounted, onUnmounted } from 'vue'
import NavHeader   from '../components/NavHeader.vue'
import KpiCard     from '../components/KpiCard.vue'
import BaseEchart  from '../components/BaseEchart.vue'
import LayoutRow   from '../components/LayoutRow.vue'
import BaseTable   from '../components/BaseTable.vue'
import DetailModal from '../components/DetailModal.vue'
import * as api    from '../api/dashboard'

// 自适应缩放: 宽度适配，高度可滚动
const CONTENT_H = 2090   // dash-wrap 实际内容总高度(px) — 表格行 460px 后重算
const scaleX = ref(1)
function updateScale() {
  // 仅按宽度缩放
  scaleX.value = Math.min(window.innerWidth / 1920, 1)
}
const dashStyle = computed(() => ({
  transform: `scale(${scaleX.value})`,
  transformOrigin: 'left top',
  // 负边距补偿: 消除 transform 不收缩 layout-box 造成的多余滚动空间
  marginBottom: -(CONTENT_H * (1 - scaleX.value)) + 'px'
}))

// ==================== 配色常量 ====================
const C = {
  honey:  '#FFD369', rose: '#FFB4A2', mint: '#B6E5C8', sky: '#B8D8F0',
  orange: '#FFA960', brown:'#5C3C24',  lbrown:'#8C6A4A',
  cream:  '#FFFCF2', border:'#FFD384'
}
const SOFT4 = [C.honey, C.rose, C.mint, C.sky]

// ==================== 通用工具 ====================
function wtip(extra) {
  return {
    backgroundColor: C.cream, borderColor: C.border, borderWidth: 1,
    borderRadius: 12, textStyle: { color: C.brown, fontSize: 17 },
    extraCssText: 'box-shadow: 0 4px 18px rgba(210,180,140,0.28);',
    ...extra
  }
}
function waxis(extra) {
  return {
    axisLabel: { color: C.lbrown, fontSize: 15 },
    axisLine:  { lineStyle: { color: C.border } },
    axisTick:  { lineStyle: { color: C.border } },
    ...extra
  }
}
function splitLine() {
  return { lineStyle: { color: 'rgba(255,211,132,0.32)', width: 1 } }
}
function yAxis(name) {
  return { type: 'value', name, nameTextStyle: { color: C.lbrown, fontSize: 15 },
           axisLabel: { color: C.lbrown, fontSize: 15 }, splitLine: splitLine() }
}
const legendStyle = { textStyle: { color: C.brown, fontSize: 16 }, bottom: 0, itemWidth: 18, itemHeight: 12 }

// ==================== 数据状态 ====================
const kpi        = reactive({ totalActive:0, minorActive:0, atRiskCount:0, severeCount:0, totalRecharge:0, avgOnlineMin:0 })
const roseOption = ref({})
const trendOption = ref({})
const periodOption = ref({})
const paymentOption = ref({})
const ranking    = ref([])
const violations = ref([])
const rankCols   = ['#','玩家ID','活跃天','总时(h)','日均(分)','充值(元)','风险']
const violationCols = ['玩家ID','游戏ID','登录时间','在线(分)','风险类型','充值(元)']
const modalVisible = ref(false)
const modalData    = ref([])

// ==================== TOP10 → 自定义列映射 ====================
function mapRanking(list) {
  return (list || []).map((r, i) => ({
    '#': i+1,
    '玩家ID': r.playerId, '活跃天': r.activeDays, '总时(h)': r.totalHours,
    '日均(分)': r.avgDailyMin, '充值(元)': r.totalRecharge,
    '风险': 'Lv' + r.maxRiskLevel + (r.maxRiskLevel===3?' 重度':r.maxRiskLevel===2?' 违规':' 预警'),
    maxRiskLevel: r.maxRiskLevel, playerId: r.playerId
  }))
}
function mapViolation(list) {
  return (list || []).map(r => ({
    '玩家ID': r.playerId, '游戏ID': r.gameId, '登录时间': r.loginTime,
    '在线(分)': r.onlineMin, '风险类型': r.riskType, '充值(元)': r.rechargeAmount,
    riskType: r.riskType, playerId: r.playerId
  }))
}

// ==================== 数据加载 ====================
async function loadKpi() {
  try { const d = await api.getKpiToday(); Object.assign(kpi, d) } catch(e) { /* silent */ }
}
async function loadRose() {
  try {
    const d = await api.getDurationDist()
    const data = d.names.map((n, i) => ({
      name: n, value: d.values[i],
      itemStyle: { color: i === d.names.length-1 ? C.orange : SOFT4[i%4], borderRadius: 10 }
    }))
    roseOption.value = {
      tooltip: wtip({ trigger:'item', formatter:'{b}: {c}人 ({d}%)' }),
      series: [{ type:'pie', radius:['32%','80%'], center:['50%','52%'], roseType:'radius', data,
        label: { color: C.brown, fontSize: 15, formatter:'{b}\n{d}%' },
        labelLine: { lineStyle:{color:C.lbrown,width:1.5} }, emphasis:{scaleSize:10}
      }]
    }
  } catch(e) {}
}
async function loadTrend() {
  try {
    const d = await api.getRiskTrend()
    trendOption.value = {
      tooltip: wtip({ trigger:'axis', axisPointer:{type:'shadow'} }),
      legend: { data:['DAU','一级预警','二级违规','重度沉迷','总体风险率','未成年风险率'], ...legendStyle },
      grid: { left:75, right:85, top:30, bottom:55 },
      xAxis: waxis({ type:'category', data:d.categories, axisLabel:{color:C.brown,fontSize:15} }),
      yAxis: [ yAxis('人数'), Object.assign(yAxis('风险率(%)'), { max:100, splitLine:{show:false} }) ],
      series: [
        { name:'DAU', type:'bar', data:d.dau, itemStyle:{color:C.honey,borderRadius:[5,5,0,0]}, barWidth:'42%', barGap:'22%' },
        { name:'一级预警', type:'bar', stack:'risk', data:d.level1, itemStyle:{color:C.mint}, barWidth:'42%' },
        { name:'二级违规', type:'bar', stack:'risk', data:d.level2, itemStyle:{color:C.sky} },
        { name:'重度沉迷', type:'bar', stack:'risk', data:d.level3, itemStyle:{color:C.orange} },
        { name:'总体风险率', type:'line', yAxisIndex:1, data:d.riskRate, lineStyle:{color:C.rose,width:3},
          symbol:'circle',symbolSize:9,itemStyle:{color:C.rose,borderColor:'#fff',borderWidth:2} },
        { name:'未成年风险率', type:'line', yAxisIndex:1, data:d.minorRiskRate, lineStyle:{color:'#E5B940',width:3,type:'dashed'},
          symbol:'diamond',symbolSize:10,itemStyle:{color:'#E5B940',borderColor:'#fff',borderWidth:2} }
      ]
    }
  } catch(e) {}
}
async function loadPeriod() {
  try {
    const d = await api.getPeriodStats()
    const hours = d.hours.map(h => h<10?'0'+h+':00':h+':00')
    periodOption.value = {
      tooltip: wtip({ trigger:'axis' }),
      legend: { data:['成年玩家','未成年玩家'], ...legendStyle },
      grid: { left:70, right:40, top:30, bottom:50 },
      xAxis: waxis({ type:'category', data:hours, axisLabel:{color:C.lbrown,fontSize:14} }),
      yAxis: yAxis('活跃人数'),
      series: [
        { name:'成年玩家', type:'line', stack:'total', areaStyle:{color:'rgba(182,229,200,0.70)'},
          lineStyle:{color:C.mint,width:3}, data:d.adultActive, symbol:'none', smooth:true,
          markArea:{ silent:true, label:{show:true,position:'insideTop',color:C.orange,fontSize:13},
            data:[[{xAxis:'00:00'},{xAxis:'07:00'}],[{xAxis:'22:00'},{xAxis:'23:00'}]],
            itemStyle:{color:'rgba(255,169,96,0.12)'} } },
        { name:'未成年玩家', type:'line', stack:'total', areaStyle:{color:'rgba(184,216,240,0.70)'},
          lineStyle:{color:C.sky,width:3}, data:d.minorActive, symbol:'none', smooth:true }
      ]
    }
  } catch(e) {}
}
// ===== 桑基图: 账号类型 → 付费区间 =====
function buildSankey(d) {
  // 节点
  const nodes = [
    { name: '成人账号',   itemStyle: { color: C.mint,  borderColor: '#8CC9A0', borderRadius: 8 } },
    { name: '未成年账号', itemStyle: { color: C.rose,  borderColor: '#E89480', borderRadius: 8 } }
  ]
  const rangeColors = [C.honey, C.sky, C.orange, C.mint, C.rose, C.honey]
  d.categories.forEach((cat, i) => {
    nodes.push({ name: cat, itemStyle: { color: rangeColors[i % 6], borderColor: 'rgba(210,180,140,0.6)', borderRadius: 8 } })
  })

  // 流转链路
  const links = []
  d.categories.forEach((cat, i) => {
    if (d.adultCount[i] > 0) links.push({ source: '成人账号', target: cat, value: d.adultCount[i] })
    if (d.minorCount[i] > 0) links.push({ source: '未成年账号', target: cat, value: d.minorCount[i] })
  })

  return { nodes, links }
}

async function loadPayment() {
  try {
    const d = await api.getPaymentAnalysis()
    const { nodes, links } = buildSankey(d)
    paymentOption.value = {
      tooltip: {
        ...wtip({}),
        trigger: 'item',
        triggerOn: 'mousemove',
        formatter: p => {
          if (p.dataType === 'edge' || p.dataType === 'node') {
            return `${p.name || (p.data.source + ' → ' + p.data.target)}<br/>付费人数: <b>${p.value || 0}</b>`
          }
          return ''
        }
      },
      series: [{
        type: 'sankey',
        layout: 'none',
        layoutIterations: 32,
        emphasis: { focus: 'adjacency' },
        data: nodes,
        links: links,
        label: { color: C.brown, fontSize: 15 },
        lineStyle: { color: 'gradient', curveness: 0.5, opacity: 0.75 },
        nodeWidth: 22,
        nodeGap: 14,
        nodeAlign: 'left',
        left: '5%', right: '12%', top: '8%', bottom: '8%'
      }]
    }
  } catch(e) {}
}
// 表格类API返回 {data:[], total:N}，需解包第二层 data
function unwrap(resp) { return Array.isArray(resp) ? resp : (resp && resp.data) || [] }

async function loadRanking() { try { ranking.value = mapRanking(unwrap(await api.getRiskRanking())) } catch(e) {} }
async function loadViolation(){ try { violations.value = mapViolation(unwrap(await api.getViolationDetail())) } catch(e) {} }

async function onRankClick(row) {
  try {
    const resp = await api.getPlayerDetail(row.playerId)
    modalData.value = Array.isArray(resp) ? resp : (resp && resp.data) || []
    modalVisible.value = true
  } catch(e) { console.error('下钻查询失败:', e) }
}

function loadAll() {
  loadKpi(); loadRose(); loadTrend(); loadPeriod(); loadPayment(); loadRanking(); loadViolation()
}

let t1, t2, t3
onMounted(() => {
  updateScale()
  loadAll()
  t1 = setInterval(loadKpi, 30000)
  t2 = setInterval(() => { loadRose(); loadTrend(); loadPeriod(); loadPayment() }, 60000)
  t3 = setInterval(() => { loadRanking(); loadViolation() }, 120000)
  window.addEventListener('resize', updateScale)
})
onUnmounted(() => { clearInterval(t1); clearInterval(t2); clearInterval(t3); window.removeEventListener('resize', updateScale) })

// 自适应
window.addEventListener('resize', () => {
  // ECharts 实例由 BaseEchart 组件内部管理
})
</script>

<style scoped>
.dash-outer {
  width: 100%; height: 100vh;
  overflow-y: auto;
  overflow-x: hidden;
}
.dash-wrap {
  width: 1920px; margin: 0 auto;
  padding: 14px 28px;
}
.panel {
  flex: 1;
  display: flex; flex-direction: column;
  background: rgba(255,237,194,0.80);
  border: 1px solid rgba(255,211,132,0.45);
  border-radius: 18px;
  padding: 18px;
  box-shadow: 0 4px 20px rgba(210,180,140,0.22);
  overflow: hidden;
}
.panel-title {
  font-size: 21px; font-weight: bold; color: #5C3C24;
  margin-bottom: 12px; padding-left: 16px;
  border-left: 5px solid #FFD384;
  flex-shrink: 0;
}
.echart-wrap { overflow: hidden; }
/* 表格行 — 比上方图表行矮约100px，内部卡片 flex 均分 */
.table-row { height: 550px; flex-shrink: 0; }
.footer-bar {
  height: 32px; display: flex; align-items: center; justify-content: center; gap: 36px;
  font-size: 15px; color: #8C6A4A; margin-top: 8px;
  flex-shrink: 0;
}
</style>
