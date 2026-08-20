# Dolphin — 构建系统

.PHONY: help proto lint test test-cover bench build build-linux docker-build infra-up infra-down run-gateway run-scheduler run-worker smoke bench-gateway bench-schedule failover k8s-images k8s-apply k8s-delete dolphinctl-image clean

APP_NAME := dolphin
REGISTRY := docker.io/$(USER)
VERSION  := $(shell git describe --tags --always --dirty 2>/dev/null || echo "dev")
BUILD_TIME := $(shell date -u '+%Y-%m-%d_%H:%M:%S')
GIT_COMMIT := $(shell git rev-parse --short HEAD 2>/dev/null || echo "unknown")

LDFLAGS := -s -w \
	-X main.Version=$(VERSION) \
	-X main.BuildTime=$(BUILD_TIME) \
	-X main.GitCommit=$(GIT_COMMIT)

GO := go

help: ## 显示帮助信息
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

proto: ## 生成 gRPC 桩代码
	@echo "Generating protobuf..."
	# module=... 使输出目录跟随 go_package（api/proto/pb），而非 proto 文件所在目录。
	protoc --go_out=. --go_opt=module=github.com/yourname/dolphin \
		--go-grpc_out=. --go-grpc_opt=module=github.com/yourname/dolphin \
		api/proto/scheduler.proto

lint: ## 运行 go vet
	@echo "Running go vet..."
	$(GO) vet ./...

fmt: ## 格式化代码
	@echo "Formatting..."
	$(GO) fmt ./...

test: ## 运行单元测试
	@echo "Running unit tests..."
	$(GO) test -race -count=1 ./...

test-cover: ## 运行测试 + 覆盖率报告
	@echo "Running tests with coverage..."
	$(GO) test -race -count=1 -coverprofile=coverage.out ./...
	$(GO) tool cover -html=coverage.out -o coverage.html
	@echo "Coverage report: coverage.html"

bench: ## 运行基准测试
	@echo "Running benchmarks..."
	$(GO) test -bench=. -benchmem -benchtime=1s ./internal/...

build: ## 编译所有二进制
	@echo "Building $(APP_NAME) v$(VERSION)..."
	$(GO) build -trimpath -ldflags="$(LDFLAGS)" -o bin/gateway    ./cmd/gateway
	$(GO) build -trimpath -ldflags="$(LDFLAGS)" -o bin/scheduler  ./cmd/scheduler
	$(GO) build -trimpath -ldflags="$(LDFLAGS)" -o bin/worker     ./cmd/worker
	$(GO) build -trimpath -ldflags="$(LDFLAGS)" -o bin/dolphinctl ./cmd/dolphinctl
	$(GO) build -trimpath -ldflags="$(LDFLAGS)" -o bin/loadgen    ./cmd/loadgen
	@echo "Build complete: $(shell ls bin/)"

build-ctl: ## 仅编译 dolphinctl
	@echo "Building dolphinctl..."
	$(GO) build -trimpath -ldflags="$(LDFLAGS)" -o bin/dolphinctl ./cmd/dolphinctl
	@echo "Built bin/dolphinctl"

build-linux: ## 交叉编译 Linux amd64
	@echo "Cross-compiling for linux/amd64..."
	GOOS=linux GOARCH=amd64 $(GO) build -trimpath -ldflags="$(LDFLAGS)" -o bin/gateway-linux    ./cmd/gateway
	GOOS=linux GOARCH=amd64 $(GO) build -trimpath -ldflags="$(LDFLAGS)" -o bin/scheduler-linux  ./cmd/scheduler
	GOOS=linux GOARCH=amd64 $(GO) build -trimpath -ldflags="$(LDFLAGS)" -o bin/worker-linux     ./cmd/worker

docker-build: ## 构建 Docker 镜像
	@echo "Building Docker images..."
	docker build -t $(REGISTRY)/dolphin-gateway:$(VERSION)   -f deployments/docker/Dockerfile.gateway .
	docker build -t $(REGISTRY)/dolphin-scheduler:$(VERSION) -f deployments/docker/Dockerfile.scheduler .
	docker build -t $(REGISTRY)/dolphin-worker:$(VERSION)    -f deployments/docker/Dockerfile.worker .

infra-up: ## 启动基础设施（etcd+MySQL+Redis）
	@echo "Starting infrastructure..."
	docker-compose -f deployments/docker-compose.yaml up -d etcd mysql redis
	@sleep 5
	@echo "Infrastructure ready."

infra-down: ## 停止基础设施
	docker-compose -f deployments/docker-compose.yaml down

run-gateway: ## 启动 Gateway
	$(GO) run ./cmd/gateway -config=configs/gateway.yaml

run-scheduler: ## 启动 Scheduler
	$(GO) run ./cmd/scheduler -config=configs/scheduler.yaml

run-worker: ## 启动 Worker
	$(GO) run ./cmd/worker -config=configs/worker.yaml

# ─── 验证与压测 ───
smoke: ## 端到端冒烟测试（需先启动基础设施和组件）
	./hack/smoke_test.sh

bench-gateway: ## 网关压测 [QPS] [DURATION]
	./hack/bench_gateway.sh $(QPS) $(DURATION)

bench-schedule: ## 调度压测 [COUNT]
	./hack/bench_schedule.sh $(COUNT)

failover: ## 故障注入测试（Worker 宕机恢复）
	./hack/test_failover.sh

bench-full: ## 完整闭环压测（Windows: powershell .\hack\bench_full.ps1）
	@echo "Windows 下运行: powershell -ExecutionPolicy Bypass .\\hack\\bench_full.ps1"

# ─── K8s 部署 ───
k8s-images: ## 构建三个组件镜像（本地 tag，便于 kind/minikube 直接使用）
	docker build -t dolphin-gateway:latest   -f deployments/docker/Dockerfile.gateway   .
	docker build -t dolphin-scheduler:latest -f deployments/docker/Dockerfile.scheduler .
	docker build -t dolphin-worker:latest    -f deployments/docker/Dockerfile.worker    .

k8s-apply: ## 部署 Dolphin 到 K8s（需 kubectl + 集群）
	kubectl apply -k deployments/k8s
	@echo "已部署。查看: kubectl -n dolphin get all"

k8s-delete: ## 卸载 K8s 部署
	kubectl delete -k deployments/k8s

dolphinctl-image: ## 构建管理 CLI 镜像（集群内跑 dolphinctl 用，非部署组件）
	docker build -t dolphinctl:latest -f deployments/docker/Dockerfile.dolphinctl .

clean: ## 清理构建产物
	rm -rf bin/
	rm -f coverage.out coverage.html
