<template>
  <div class="kpi-card" :class="{ 'kpi-warn': warn, 'kpi-danger': danger }">
    <span class="kpi-label">{{ label }}</span>
    <span class="kpi-value">{{ displayValue }}</span>
    <span class="kpi-unit">{{ unit }}</span>
  </div>
</template>

<script setup>
const props = defineProps({
  label:   { type: String, required: true },
  value:   { type: [Number, String], default: 0 },
  unit:    { type: String, default: '' },
  warn:    { type: Boolean, default: false },
  danger:  { type: Boolean, default: false },
  prefix:  { type: String, default: '' }
})

import { computed } from 'vue'
const displayValue = computed(() => {
  const v = props.value
  if (typeof v === 'number') return props.prefix + v.toLocaleString()
  return props.prefix + v
})
</script>

<style scoped>
.kpi-card {
  flex: 1; height: 165px;
  display: flex; flex-direction: column; justify-content: center; align-items: center;
  background: linear-gradient(180deg, #FFF2CC 0%, #FFE6B3 100%);
  border: 1px solid #FFD384;
  border-radius: 22px;
  position: relative; overflow: hidden;
  box-shadow: 0 4px 20px rgba(255,211,132,0.35);
  padding: 22px;
}
.kpi-card::after {
  content: ''; position: absolute; top: 0; left: 10%; width: 80%; height: 100%;
  background: radial-gradient(ellipse at center, rgba(255,255,240,0.55) 0%, transparent 70%);
  pointer-events: none;
}
.kpi-label { font-size: 19px; color: #8C6A4A; margin-bottom: 10px; z-index: 1; }
.kpi-value { font-size: 45px; font-weight: bold; color: #5C3C24; z-index: 1; }
.kpi-unit  { font-size: 15px; color: #8C6A4A; margin-top: 6px; z-index: 1; }
.kpi-warn  .kpi-value { color: #FF9F40; }
.kpi-danger .kpi-value { color: #FF9F40; animation: kpi-blink 1.8s infinite; }
@keyframes kpi-blink { 50% { opacity: 0.55; } }
</style>
