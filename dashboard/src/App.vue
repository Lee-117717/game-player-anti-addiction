<template>
  <Dashboard />
</template>

<script setup>
import Dashboard from './views/Dashboard.vue'
</script>

<style>
/* ============================================================ */
/* 全局暖黄柔和色系 — 标准大数据大屏观感                         */
/* 色值: #FFF7E6(米杏底) #FFEDC2(奶黄容器) #5C3C24(暖棕字)     */
/* ============================================================ */
* { margin: 0; padding: 0; box-sizing: border-box; }
html, body {
  width: 100%; height: 100%;
  margin: 0; padding: 0;
  overflow: hidden;
  background: #FFF7E6;
  font-family: 'Microsoft YaHei', 'PingFang SC', sans-serif;
}
#app { width: 100%; height: 100%; overflow: hidden; }
</style>
