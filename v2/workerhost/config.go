package workerhost

import (
	"fmt"

	"github.com/davidroman0O/gostage/v2/broker"
	"github.com/davidroman0O/gostage/v2/registry"
	"github.com/davidroman0O/gostage/v2/runner"
	"github.com/davidroman0O/gostage/v2/runtime/core"
)

// Config describes how to construct a WorkerHost instance.
type Config struct {
	InitialSlots  int
	Factory       core.Factory
	Registry      registry.Registry
	BrokerBuilder func() (broker.Broker, error)
	RunnerOptions []runner.Option
}

func (c Config) validate() error {
	if c.InitialSlots <= 0 {
		return fmt.Errorf("workerhost: InitialSlots must be >= 1")
	}
	if c.Factory == nil {
		return fmt.Errorf("workerhost: Factory is required")
	}
	if c.Registry == nil {
		return fmt.Errorf("workerhost: Registry is required")
	}
	if c.BrokerBuilder == nil {
		return fmt.Errorf("workerhost: BrokerBuilder is required")
	}
	return nil
}
