<template>
  <teleport to="body">
    <div v-if="visible" class="modal-mask" @click.self="close">
      <div class="modal-body">
        <h3>玩家行为明细（{{ data.length }}条）</h3>
        <button class="modal-close-btn" @click="close">关闭</button>
        <table>
          <thead>
            <tr>
              <th>日期</th><th>玩家</th><th>登录时间</th><th>在线(分)</th>
              <th>设备</th><th>区域</th><th>游戏</th><th>充值</th><th>风险</th>
            </tr>
          </thead>
          <tbody>
            <tr v-for="(r, i) in data" :key="i">
              <td>{{ r.dt }}</td><td>{{ r.playerId }}</td>
              <td>{{ r.loginTime || '' }}</td><td>{{ r.onlineMin }}</td>
              <td>{{ r.deviceType || '' }}</td><td>{{ r.gameRegion || '' }}</td>
              <td>{{ r.gameId || '' }}</td>
              <td>¥{{ (r.rechargeAmount || 0).toFixed(0) }}</td>
              <td class="risk-l3">{{ r.riskDesc || '' }}</td>
            </tr>
          </tbody>
        </table>
      </div>
    </div>
  </teleport>
</template>

<script setup>
const props = defineProps({
  visible: { type: Boolean, default: false },
  data:    { type: Array, default: () => [] }
})
const emit = defineEmits(['close'])
function close() { emit('close') }
</script>

<style scoped>
.modal-mask {
  position: fixed; inset: 0; z-index: 999;
  background: rgba(140,106,74,0.35);
  display: flex; align-items: center; justify-content: center;
}
.modal-body {
  width: 84%; max-height: 88%;
  background: #FFFCF2; border: 2px solid #FFD384; border-radius: 22px;
  padding: 28px; color: #5C3C24; overflow: auto; font-size: 15px;
  box-shadow: 0 10px 50px rgba(180,140,100,0.38);
}
.modal-body h3 { font-size: 20px; color: #5C3C24; margin-bottom: 16px; }
.modal-close-btn {
  float: right; background: #FF9F40; color: #fff; border: none;
  padding: 8px 22px; border-radius: 12px; cursor: pointer; font-size: 16px;
  box-shadow: 0 3px 10px rgba(255,159,64,0.32);
}
.modal-body table { margin-top: 44px; width: 100%; font-size: 15px; }
.modal-body th { background: #FFE6B3; color: #5C3C24; padding: 10px 8px; }
.modal-body td { padding: 10px 8px; border-bottom: 1px solid rgba(255,211,132,0.35); }
.risk-l3 { color: #FF9F40; font-weight: 700; }
</style>
