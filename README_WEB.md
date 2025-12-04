# Web 端使用说明

本项目包含一个基于 Vue 3 的 Web 前端和 FastAPI 后端，用于通过 Web 界面进行数据同步。

## 项目结构

```
event-driven-crypto/
├── backend/          # FastAPI 后端
│   ├── __init__.py
│   └── api.py       # API 接口
├── frontend/         # Vue 3 前端
│   ├── src/
│   │   ├── components/
│   │   │   └── DataSync.vue  # 数据同步组件
│   │   ├── App.vue
│   │   └── main.js
│   ├── index.html
│   ├── package.json
│   └── vite.config.js
└── src/              # 原有 Python 代码
```

## 安装和运行

### 1. 安装后端依赖

```bash
pip install -r requirements.txt
```

### 2. 启动后端服务

```bash
# 方式 1: 直接运行
python backend/api.py

# 方式 2: 使用 uvicorn
uvicorn backend.api:app --host 0.0.0.0 --port 8000
```

后端服务将在 `http://localhost:8000` 启动。

### 3. 安装前端依赖

```bash
cd frontend
npm install
```

### 4. 启动前端开发服务器

```bash
cd frontend
npm run dev
```

前端服务将在 `http://localhost:3000` 启动。

## 使用说明

1. 打开浏览器访问 `http://localhost:3000`
2. 在数据同步页面：
   - 选择开始时间和结束时间
   - 选择 K 线周期（默认 1 小时）
   - 点击"开始同步"按钮
3. 等待同步完成，查看同步结果和历史记录

## API 文档

后端启动后，可以访问以下地址查看自动生成的 API 文档：

- Swagger UI: `http://localhost:8000/docs`
- ReDoc: `http://localhost:8000/redoc`

## 功能特性

- ✅ 可视化时间选择器
- ✅ 实时同步状态显示
- ✅ 同步历史记录
- ✅ 错误提示和成功反馈
- ✅ 响应式设计，支持移动端

## 注意事项

1. 确保后端服务在 `http://localhost:8000` 运行
2. 前端通过代理访问后端 API（配置在 `vite.config.js` 中）
3. 生产环境部署时，需要配置正确的 CORS 策略

