# 🐕 NaviDog

一个轻量级的 Web 端 MySQL 管理工具，风格类似 Navicat。

## 功能

- 🔗 **连接管理** — 支持 SSH 隧道（密码/密钥）
- 📥 **NCX 导入/导出** — 浏览器端直接解析 Navicat 连接文件（含密码解密）
- 📊 **数据浏览** — 表格数据查看、筛选、排序、分页
- 📝 **SQL 编辑器** — 语法高亮、自动补全、多结果集（消息/摘要视图）
- 📋 **表管理** — 复制表（结构/数据）、删除表、清空表
- 💾 **数据导出** — 转储单表/整库为 SQL 文件
- 📄 **DDL 查看** — SQL 语法高亮显示
- 🖥️ **面板控制** — 左/右面板可折叠、可拖拽调整大小

## 快速开始

### 方式一：npx 直接用

```bash
npx navidog
```

### 方式二：全局安装

```bash
npm install -g navidog
navidog
```

### 方式三：克隆源码

```bash
git clone https://github.com/fhyfhy17/navidog.git
cd navidog
npm install
npm run build
npm start
```

启动后打开 http://127.0.0.1:3001 即可使用。

### 开发模式

```bash
npm install
npm run dev
```

前端页面：http://localhost:5173 （自动代理 API 到后端 3001 端口）

### 自定义端口

```bash
PORT=8080 npm start
# 或
PORT=8080 navidog
```

## 使用方式

1. 打开浏览器访问 `http://127.0.0.1:3001`
2. 点击工具栏「连接」创建 MySQL 连接
3. 填写主机、端口、用户名、密码（支持 SSH 隧道）
4. 也可以点「导入连接」导入 Navicat 的 `.ncx` 文件
5. 连接后左侧树浏览数据库和表
6. 双击表名查看数据，右键有更多操作（复制/删除/清空/转储）
7. 点击「新建查询」打开 SQL 编辑器

## 安全说明

- 服务默认绑定 `127.0.0.1`，仅本机可访问
- 连接密码存储在浏览器 localStorage 中
- 不建议将服务暴露到公网

## 技术栈

- **前端**: React 19 + TypeScript + Vite + CodeMirror 6
- **后端**: Express 5 + mysql2 + ssh2
- **部署**: 单进程同时服务 API 和前端静态文件（无需 nginx）

## License

MIT
