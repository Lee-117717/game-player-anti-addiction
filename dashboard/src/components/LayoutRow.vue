<template>
  <div class="layout-row">
    <slot />
  </div>
</template>

<script setup>
defineProps({ cols: { type: Number, default: 2 } })
</script>

<style scoped>
.layout-row {
  display: flex; gap: 30px; margin-bottom: 18px;
}
/* 直接子元素均分 */
.layout-row > :deep(*) { flex: 1; min-width: 0; }
</style>
