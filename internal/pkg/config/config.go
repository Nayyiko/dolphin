package config

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

// Load 从文件加载 YAML 配置到 out。
func Load(path string, out any) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("read config %s: %w", path, err)
	}
	if err := yaml.Unmarshal(data, out); err != nil {
		return fmt.Errorf("parse config %s: %w", path, err)
	}
	return nil
}
