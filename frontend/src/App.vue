<template>
  <div id="app">
    <el-container class="app-container">
      <el-header class="app-header">
        <h1>Event-Driven Crypto - 数据同步系统</h1>
      </el-header>
      <el-main class="app-main">
        <DataSync />
      </el-main>
    </el-container>
  </div>
</template>

<script>
import DataSync from './components/DataSync.vue'

export default {
  name: 'App',
  components: {
    DataSync
  }
}
</script>

<style>
* {
  margin: 0;
  padding: 0;
  box-sizing: border-box;
}

#app {
  font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, 'Helvetica Neue', Arial, sans-serif;
  min-height: 100vh;
  background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
}

.app-container {
  min-height: 100vh;
}

.app-header {
  background: rgba(255, 255, 255, 0.95);
  box-shadow: 0 2px 12px rgba(0, 0, 0, 0.1);
  display: flex;
  align-items: center;
  justify-content: center;
  padding: 0 20px;
}

.app-header h1 {
  color: #333;
  font-size: 24px;
  font-weight: 600;
}

.app-main {
  padding: 40px 20px;
  max-width: 1200px;
  margin: 0 auto;
  width: 100%;
}
</style>

