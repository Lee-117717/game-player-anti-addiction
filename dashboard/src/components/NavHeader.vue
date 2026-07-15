<template>
  <div class="nav-header">
    <span class="nav-title">游戏平台玩家行为与防沉迷监控可视化大屏</span>
    <span class="nav-time">{{ timeStr }}</span>
  </div>
</template>

<script setup>
import { ref, onMounted, onUnmounted } from 'vue'

const timeStr = ref('')
let timer = null

onMounted(() => {
  const tick = () => {
    timeStr.value = new Date().toLocaleString('zh-CN', { hour12: false })
  }
  tick()
  timer = setInterval(tick, 1000)
})
onUnmounted(() => clearInterval(timer))
</script>

<style scoped>
.nav-header {
  width: 100%; height: 80px; min-height: 80px;
  display: flex; align-items: center; justify-content: center;
  position: relative;
  padding: 0 32px;
  background: linear-gradient(90deg, rgba(255,237,194,0.92), rgba(255,247,230,0.65), rgba(255,237,194,0.92));
  border-radius: 16px;
  box-shadow: 0 2px 14px rgba(255,211,132,0.28);
  margin-bottom: 18px;
}
.nav-title {
  font-size: 30px; font-weight: bold; color: #5C3C24;
  letter-spacing: 5px; flex: 1; text-align: center;
}
.nav-time {
  position: absolute; right: 48px;
  font-size: 16px; color: #8C6A4A;
}
</style>
