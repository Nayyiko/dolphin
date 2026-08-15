package config

import "time"

// GatewayConfig 网关配置。
type GatewayConfig struct {
	Server struct {
		Port         int           `yaml:"port"`
		ReadTimeout  time.Duration `yaml:"read_timeout"`
		WriteTimeout time.Duration `yaml:"write_timeout"`
		IdleTimeout  time.Duration `yaml:"idle_timeout"`
	} `yaml:"server"`
	Redis struct {
		Addr     string `yaml:"addr"`
		Password string `yaml:"password"`
		DB       int    `yaml:"db"`
	} `yaml:"redis"`
	JWT struct {
		Secret      string `yaml:"secret"`
		ExpireHours int    `yaml:"expire_hours"`
	} `yaml:"jwt"`
	RateLimit struct {
		Enabled        bool  `yaml:"enabled"`
		DefaultRate    int   `yaml:"default_rate"`    // tokens/sec
		DefaultCap     int   `yaml:"default_capacity"` // burst
		WindowSeconds  int64 `yaml:"window_seconds"`  // key 过期窗口
	} `yaml:"rate_limit"`
	CircuitBreaker struct {
		Enabled          bool          `yaml:"enabled"`
		FailureThreshold int           `yaml:"failure_threshold"`
		Timeout          time.Duration `yaml:"timeout"`
		HalfOpenMax      int           `yaml:"half_open_max"`
	} `yaml:"circuit_breaker"`
	Scheduler struct {
		Addr string `yaml:"addr"` // gRPC 地址
	} `yaml:"scheduler"`
}

// SchedulerConfig 调度器配置。
type SchedulerConfig struct {
	Server struct {
		GRPCPort int    `yaml:"grpc_port"`
		HTTPPort int    `yaml:"http_port"` // metrics / health
		// AdvertiseAddr 发布给 Worker 的 Leader gRPC 地址。
		// 默认空 → 用 localhost:<grpc_port>（单机/容器同宿主机场景）。
		// 多机/K8s 场景必须显式配置可达地址（如 Pod IP:50051），
		// 否则 Worker 通过 etcd 发现的 leader-addr 连不上自己所在的 localhost。
		AdvertiseAddr string `yaml:"advertise_addr"`
	} `yaml:"server"`
	Etcd struct {
		Endpoints   []string      `yaml:"endpoints"`
		DialTimeout time.Duration `yaml:"dial_timeout"`
	} `yaml:"etcd"`
	Election struct {
		TTL int `yaml:"ttl"` // Lease TTL 秒
	} `yaml:"election"`
	MySQL struct {
		DSN string `yaml:"dsn"`
	} `yaml:"mysql"`
	Redis struct {
		Addr     string `yaml:"addr"`
		Password string `yaml:"password"`
		DB       int    `yaml:"db"`
	} `yaml:"redis"`
	Reconciler struct {
		Workers int `yaml:"workers"`
	} `yaml:"reconciler"`
	Failover struct {
		HeartbeatTimeout time.Duration `yaml:"heartbeat_timeout"`
		MaxRetries       int           `yaml:"max_retries"`
		// RetryBaseDelay 执行级重试基础退避：attempt n 延迟 = base * 2^(n-1)。
		RetryBaseDelay time.Duration `yaml:"retry_base_delay"`
		// StaleRunningGrace running 日志超过 task.timeout + grace 视为结果丢失并兜底救援。
		StaleRunningGrace time.Duration `yaml:"stale_running_grace"`
	} `yaml:"failover"`
}

// WorkerConfig Worker 配置。
type WorkerConfig struct {
	Server struct {
		MetricsPort int `yaml:"metrics_port"`
	} `yaml:"server"`
	Scheduler struct {
		Addr string `yaml:"addr"`
	} `yaml:"scheduler"`
	Etcd struct {
		Endpoints   []string      `yaml:"endpoints"`
		DialTimeout time.Duration `yaml:"dial_timeout"`
	} `yaml:"etcd"`
	Pool struct {
		Capacity int `yaml:"capacity"`
	} `yaml:"pool"`
	Heartbeat struct {
		Interval time.Duration `yaml:"interval"`
	} `yaml:"heartbeat"`
}
