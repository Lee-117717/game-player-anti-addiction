import axios from 'axios'

const http = axios.create({
  baseURL: '/api',
  timeout: 15000
})

// 响应拦截: 统一解包 Result
http.interceptors.response.use(
  res => {
    const d = res.data
    if (d.code === 200) return d.data
    return Promise.reject(new Error(d.message || '请求失败'))
  },
  err => Promise.reject(err)
)

/** 统一 GET 请求 */
function get(url, params = {}) {
  return http.get(url, { params })
}

// ==================== 8 组数据接口 ====================

/** 1. 当日核心KPI */
export function getKpiToday() {
  return get('/kpi/today')
}

/** 2. 在线时长分布 */
export function getDurationDist() {
  return get('/duration/dist')
}

/** 3. 7天风险趋势 */
export function getRiskTrend() {
  return get('/risk/trend')
}

/** 4. 付费分析 */
export function getPaymentAnalysis() {
  return get('/payment/analysis')
}

/** 5. 24小时时段统计 */
export function getPeriodStats() {
  return get('/period/stats')
}

/** 6. 高风险玩家Top10 */
export function getRiskRanking() {
  return get('/risk/ranking')
}

/** 7. 违规玩家明细 */
export function getViolationDetail() {
  return get('/violation/detail')
}

/** 8. 未成年夜间违规明细 */
export function getMinorNightDetail() {
  return get('/minor/night-detail')
}

/** 9. 指定玩家明细 (TOP10下钻) */
export function getPlayerDetail(playerId) {
  return get('/player/detail', { playerId })
}
