<template>
  <div class="data-sync">
    <el-card class="sync-card" shadow="hover">
      <template #header>
        <div class="card-header">
          <span class="card-title">ETH 数据同步</span>
          <el-tag :type="statusTagType" size="large">{{ statusText }}</el-tag>
        </div>
      </template>

      <el-form
        ref="formRef"
        :model="form"
        :rules="rules"
        label-width="120px"
        label-position="left"
        class="sync-form"
      >
        <el-form-item label="开始时间" prop="start">
          <el-date-picker
            v-model="form.start"
            type="datetime"
            placeholder="选择开始时间"
            format="YYYY-MM-DD HH:mm:ss"
            value-format="YYYY-MM-DD HH:mm:ss"
            style="width: 100%"
            :disabled-date="disabledDate"
          />
        </el-form-item>

        <el-form-item label="结束时间" prop="end">
          <el-date-picker
            v-model="form.end"
            type="datetime"
            placeholder="选择结束时间"
            format="YYYY-MM-DD HH:mm:ss"
            value-format="YYYY-MM-DD HH:mm:ss"
            style="width: 100%"
            :disabled-date="disabledDate"
          />
        </el-form-item>

        <el-form-item label="K 线周期" prop="interval">
          <el-select v-model="form.interval" placeholder="选择 K 线周期" style="width: 100%">
            <el-option label="1 分钟" value="1m" />
            <el-option label="5 分钟" value="5m" />
            <el-option label="15 分钟" value="15m" />
            <el-option label="30 分钟" value="30m" />
            <el-option label="1 小时" value="1h" />
            <el-option label="4 小时" value="4h" />
            <el-option label="1 天" value="1d" />
          </el-select>
        </el-form-item>

        <el-form-item>
          <el-button
            type="primary"
            :loading="loading"
            @click="handleSync"
            size="large"
            style="width: 100%"
          >
            {{ loading ? '同步中...' : '开始同步' }}
          </el-button>
        </el-form-item>
      </el-form>

      <!-- 结果显示 -->
      <el-alert
        v-if="result"
        :title="result.title"
        :type="result.type"
        :description="result.message"
        show-icon
        :closable="true"
        @close="result = null"
        style="margin-top: 20px"
      />
    </el-card>

    <!-- 同步历史记录 -->
    <el-card class="history-card" shadow="hover" style="margin-top: 20px">
      <template #header>
        <div class="card-header">
          <span class="card-title">同步历史</span>
          <el-button text @click="clearHistory">清空历史</el-button>
        </div>
      </template>
      <el-timeline v-if="history.length > 0">
        <el-timeline-item
          v-for="(item, index) in history"
          :key="index"
          :timestamp="item.timestamp"
          :type="item.type"
        >
          <div class="history-item">
            <p><strong>{{ item.title }}</strong></p>
            <p>{{ item.message }}</p>
            <p v-if="item.recordsCount" class="records-count">
              记录数: {{ item.recordsCount }}
            </p>
          </div>
        </el-timeline-item>
      </el-timeline>
      <el-empty v-else description="暂无同步记录" />
    </el-card>
  </div>
</template>

<script>
import { ref, reactive, computed } from 'vue'
import { ElMessage } from 'element-plus'
import axios from 'axios'

export default {
  name: 'DataSync',
  setup() {
    const formRef = ref(null)
    const loading = ref(false)
    const result = ref(null)
    const history = ref([])

    // 表单数据
    const form = reactive({
      start: '',
      end: '',
      interval: '1h'
    })

    // 表单验证规则
    const validateEndTime = (rule, value, callback) => {
      if (!value) {
        callback(new Error('请选择结束时间'))
      } else if (form.start && value <= form.start) {
        callback(new Error('结束时间必须大于开始时间'))
      } else {
        callback()
      }
    }

    const rules = {
      start: [
        { required: true, message: '请选择开始时间', trigger: 'change' }
      ],
      end: [
        { required: true, validator: validateEndTime, trigger: 'change' }
      ],
      interval: [
        { required: true, message: '请选择 K 线周期', trigger: 'change' }
      ]
    }

    // 禁用未来日期
    const disabledDate = (time) => {
      return time.getTime() > Date.now()
    }

    // 状态文本和类型
    const statusText = computed(() => {
      if (loading.value) return '同步中'
      if (result.value) {
        return result.value.type === 'success' ? '同步成功' : '同步失败'
      }
      return '待同步'
    })

    const statusTagType = computed(() => {
      if (loading.value) return 'warning'
      if (result.value) {
        return result.value.type === 'success' ? 'success' : 'danger'
      }
      return 'info'
    })

    // 处理同步
    const handleSync = async () => {
      if (!formRef.value) return

      try {
        // 验证表单
        await formRef.value.validate()
      } catch (error) {
        ElMessage.error('请填写完整的表单信息')
        return
      }

      loading.value = true
      result.value = null

      try {
        // 调用后端 API
        const response = await axios.post('/api/sync', {
          start: form.start,
          end: form.end,
          interval: form.interval
        })

        const data = response.data

        if (data.success) {
          result.value = {
            type: 'success',
            title: '同步成功',
            message: data.message
          }

          // 添加到历史记录
          history.value.unshift({
            timestamp: new Date().toLocaleString('zh-CN'),
            type: 'success',
            title: '同步成功',
            message: data.message,
            recordsCount: data.records_count
          })

          ElMessage.success(data.message)
        } else {
          result.value = {
            type: 'error',
            title: '同步失败',
            message: data.error || data.message
          }

          // 添加到历史记录
          history.value.unshift({
            timestamp: new Date().toLocaleString('zh-CN'),
            type: 'danger',
            title: '同步失败',
            message: data.error || data.message
          })

          ElMessage.error(data.error || data.message)
        }
      } catch (error) {
        const errorMessage = error.response?.data?.detail || error.message || '网络错误，请检查后端服务是否启动'
        
        result.value = {
          type: 'error',
          title: '同步失败',
          message: errorMessage
        }

        // 添加到历史记录
        history.value.unshift({
          timestamp: new Date().toLocaleString('zh-CN'),
          type: 'danger',
          title: '同步失败',
          message: errorMessage
        })

        ElMessage.error(errorMessage)
      } finally {
        loading.value = false
      }
    }

    // 清空历史记录
    const clearHistory = () => {
      history.value = []
      ElMessage.success('历史记录已清空')
    }

    return {
      formRef,
      form,
      rules,
      loading,
      result,
      history,
      disabledDate,
      statusText,
      statusTagType,
      handleSync,
      clearHistory
    }
  }
}
</script>

<style scoped>
.data-sync {
  max-width: 800px;
  margin: 0 auto;
}

.sync-card,
.history-card {
  background: rgba(255, 255, 255, 0.95);
  border-radius: 12px;
}

.card-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
}

.card-title {
  font-size: 18px;
  font-weight: 600;
  color: #333;
}

.sync-form {
  margin-top: 20px;
}

.history-item {
  padding: 10px 0;
}

.history-item p {
  margin: 5px 0;
  color: #666;
}

.records-count {
  color: #409eff;
  font-weight: 500;
}
</style>

