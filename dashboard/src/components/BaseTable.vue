<template>
  <div class="bt-panel" :class="{ 'bt-compact': compact }">
    <div class="bt-title">{{ title }}</div>
    <div
      ref="wrapRef"
      class="bt-wrap"
      :class="{ 'bt-scroll': scroll }"
      @mouseenter="onHover(true)"
      @mouseleave="onHover(false)"
    >
      <table>
        <thead>
          <tr><th v-for="c in columns" :key="c">{{ c }}</th></tr>
        </thead>
        <tbody ref="tbodyRef">
          <tr v-for="(row, i) in renderData" :key="i"
            :class="getRowClass(row)"
            :style="rowStyle(row)"
            :title="rowTitle(row)"
            @click="$emit('rowClick', row)">
            <td v-for="c in columns" :key="c" :class="cellClass(row, c)">
              {{ formatCell(row, c) }}
            </td>
          </tr>
        </tbody>
      </table>
    </div>
  </div>
</template>

<script setup>
import { ref, computed, onMounted, onUnmounted, watch, nextTick } from 'vue'

const props = defineProps({
  title:      { type: String, default: '' },
  columns:    { type: Array, required: true },
  data:       { type: Array, default: () => [] },
  /** 开启自动滚动 (违规明细) */
  scroll:     { type: Boolean, default: false },
  /** 可视行数 */
  maxRows:    { type: Number, default: 0 },
  riskField:  { type: String, default: null },
  rowClickable:{ type: Boolean, default: false }
})

defineEmits(['rowClick'])

const tbodyRef = ref(null)
const wrapRef  = ref(null)
const rowH     = ref(48)
const paused   = ref(false)
const compact  = computed(() => props.maxRows > 0)

let rafId  = null
let scrollPx = 0
let lastTs   = 0

// ===== 渲染数据 =====
const renderData = computed(() => {
  // 全量渲染 — 非滚动表格靠 overflow-y:auto 内部滚动，滚动表格靠 RAF 动画
  return props.data
})

// ===== row helpers =====
function getRowClass(row) {
  if (!props.riskField) return ''
  const v = row[props.riskField]
  if (typeof v === 'string') {
    if (v.includes('重度')) return 'risk-l3'
    if (v.includes('夜间') || v.includes('违规')) return 'risk-l2'
  }
  if (v >= 3) return 'risk-l3'
  if (v >= 2) return 'risk-l2'
  if (v >= 1) return 'risk-l1'
  return ''
}
function cellClass(row, col) {
  if (col === props.riskField) return getRowClass(row)
  return ''
}
function formatCell(row, col) {
  const v = row[col]
  if (v == null) return '--'
  if (col === '充值(元)' || col === 'totalRecharge' || col === 'rechargeAmount') return '¥' + Number(v).toFixed(0)
  if (col === '登录时间' || col === 'loginTime') return String(v).substring(11, 16)
  return v
}
function rowStyle(row) {
  if (props.rowClickable) return { cursor: 'pointer' }
  return {}
}
function rowTitle(row) {
  if (props.rowClickable && row.playerId) return '点击查看 ' + row.playerId + ' 夜间违规明细'
  return ''
}

// ===== hover pause =====
function onHover(enter) {
  paused.value = enter
}

// ===== measure row height =====
function measure() {
  if (!tbodyRef.value) return
  const tr = tbodyRef.value.querySelector('tr')
  if (tr) rowH.value = tr.offsetHeight || 48
}

// ===== smooth RAF auto-scroll =====
const SCROLL_SPEED = 15  // px/s, 匀速柔和

function startScroll() {
  stopScroll()
  if (!props.scroll || props.data.length === 0) return
  scrollPx = 0
  lastTs = 0
  rafId = requestAnimationFrame(tick)
}

function tick(ts) {
  if (!tbodyRef.value || !wrapRef.value) { rafId = requestAnimationFrame(tick); return }

  if (!paused.value) {
    if (!lastTs) lastTs = ts
    const dt = Math.min((ts - lastTs) / 1000, 0.12)  // cap 120ms
    lastTs = ts

    const totalH  = tbodyRef.value.scrollHeight
    const visibleH = props.maxRows > 0 ? props.maxRows * rowH.value : wrapRef.value.clientHeight
    if (totalH > visibleH) {
      const maxScroll = totalH - visibleH + rowH.value  // +1行缓冲，无缝循环
      scrollPx += SCROLL_SPEED * dt
      if (scrollPx >= maxScroll) scrollPx = 0
      tbodyRef.value.style.transform = `translateY(-${scrollPx}px)`
    }
  }
  rafId = requestAnimationFrame(tick)
}

function stopScroll() {
  if (rafId) { cancelAnimationFrame(rafId); rafId = null }
  lastTs = 0
}

// ===== lifecycle =====
let ro = null
onMounted(() => {
  nextTick(() => {
    measure()
    if (props.scroll && props.maxRows > 0) {
      // 设置容器固定高度 = maxRows * rowH
      if (wrapRef.value) {
        wrapRef.value.style.height = (props.maxRows * rowH.value) + 'px'
        wrapRef.value.style.flex = 'none'
      }
    }
    if (props.scroll) startScroll()
    if (window.ResizeObserver && wrapRef.value) {
      ro = new ResizeObserver(() => {
        measure()
        if (props.scroll && props.maxRows > 0 && wrapRef.value) {
          wrapRef.value.style.height = (props.maxRows * rowH.value) + 'px'
        }
      })
      ro.observe(wrapRef.value)
    }
  })
})
onUnmounted(() => {
  stopScroll()
  if (ro) { ro.disconnect(); ro = null }
})

watch(() => props.data, () => {
  nextTick(() => {
    measure()
    if (props.scroll && props.maxRows > 0 && wrapRef.value) {
      wrapRef.value.style.height = (props.maxRows * rowH.value) + 'px'
    }
    if (props.scroll) startScroll()
  })
})
</script>

<style scoped>
/* ===== panel ===== */
.bt-panel {
  flex: 1;
  display: flex; flex-direction: column;
  min-height: 0;
  background: rgba(255,237,194,0.80);
  border: 1px solid rgba(255,211,132,0.45);
  border-radius: 18px;
  padding: 18px;
  box-shadow: 0 4px 20px rgba(210,180,140,0.22);
  overflow: hidden;
}
/* maxRows 模式缩小内边距 */
.bt-compact {
  padding: 12px;
}
.bt-title {
  font-size: 21px; font-weight: bold; color: #5C3C24;
  margin-bottom: 12px; padding-left: 16px;
  border-left: 5px solid #FFD384;
  flex-shrink: 0;
}
.bt-compact .bt-title {
  margin-bottom: 8px;
}

/* ===== table wrapper ===== */
.bt-wrap {
  flex: 1; min-height: 0;
  overflow-y: auto;      /* ★ 非滚动表格: 内部小滚动条 */
  overflow-x: hidden;
  border-radius: 12px;
}
.bt-scroll {
  overflow-y: hidden;    /* ★ 滚动表格: RAF 动画, 关闭原生滚动 */
  overflow-x: hidden;
}

/* ===== table ===== */
table {
  width: 100%;
  border-collapse: separate; border-spacing: 0;
  font-size: 18px; color: #5C3C24;
  table-layout: auto;
}
thead th {
  background: #FFE6B3; color: #5C3C24;
  padding: 18px 8px;
  font-size: 19px; font-weight: 600; text-align: center;
  position: sticky; top: 0; z-index: 1;
  border-radius: 8px 8px 0 0;
}
tbody td {
  padding: 16px 8px;
  text-align: center;
  border-bottom: 1px solid rgba(255,211,132,0.35);
  vertical-align: middle;
}
.bt-compact tbody td {
  padding: 12px 8px;
}
tr:nth-child(odd) td  { background: #FFF7E6; }
tr:nth-child(even) td { background: #FFFCF2; }
tr:hover td { background: rgba(255,211,132,0.18); }

/* ===== risk badges ===== */
.risk-l1 { color: #C8956C; font-weight: 500; }
.risk-l2 { color: #E89550; font-weight: 600; }
.risk-l3 { color: #FF9F40; font-weight: 700; background: rgba(255,159,64,0.08); border-radius: 8px; padding: 3px 8px; }
</style>
