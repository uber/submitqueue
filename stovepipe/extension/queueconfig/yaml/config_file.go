// Copyright (c) 2026 Uber Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package yaml

import (
	"fmt"
	"io"
	"os"

	"github.com/uber/submitqueue/stovepipe/core/queueconfig"
	yamlv3 "gopkg.in/yaml.v3"
)

type fileConfig struct {
	SharedStages   queueconfig.StageOverrides `yaml:"stages"`
	QueueOverrides []queueOverride            `yaml:"queues"`
}

type queueOverride struct {
	QueueName string                     `yaml:"name"`
	Stages    queueconfig.StageOverrides `yaml:"stages"`
}

func readConfigFile(path string) (fileConfig, error) {
	file, err := os.Open(path)
	if err != nil {
		return fileConfig{}, fmt.Errorf("open queue configuration %q: %w", path, err)
	}
	defer file.Close()

	cfg, err := decodeConfig(file)
	if err != nil {
		return fileConfig{}, fmt.Errorf("decode queue configuration %q: %w", path, err)
	}

	return cfg, nil
}

func decodeConfig(reader io.Reader) (fileConfig, error) {
	decoder := yamlv3.NewDecoder(reader)
	decoder.KnownFields(true)

	var cfg fileConfig
	if err := decoder.Decode(&cfg); err != nil && err != io.EOF {
		return fileConfig{}, err
	}

	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		if err != nil {
			return fileConfig{}, err
		}

		return fileConfig{}, fmt.Errorf("configuration must contain exactly one YAML document")
	}

	return cfg, nil
}
