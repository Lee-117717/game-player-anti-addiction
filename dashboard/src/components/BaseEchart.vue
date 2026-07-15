<template>
  <div ref="chartRef" :style="{ width: '100%', height: height }"></div>
</template>

<script setup>
import { ref, onMounted, onUnmounted, watch, nextTick } from 'vue'
import * as echarts from 'echarts'

const props = defineProps({
  option:  { type: Object, required: true },
  height:  { type: String, default: '440px' }
})

const chartRef = ref(null)
let chart = null

function initChart() {
  if (!chartRef.value) return
  if (chart) chart.dispose()
  chart = echarts.init(chartRef.value)
  chart.setOption(props.option, true)
}

onMounted(() => nextTick(initChart))

watch(() => props.option, () => {
  if (chart) chart.setOption(props.option, true)
}, { deep: true })

onUnmounted(() => { if (chart) { chart.dispose(); chart = null } })

function resize() { chart?.resize() }
defineExpose({ resize })
</script>
