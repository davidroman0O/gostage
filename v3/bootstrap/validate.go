package bootstrap

import (
	"errors"
	"fmt"
)

var (
	ErrDuplicatePool         = errors.New("gostage: duplicate pool name")
	ErrInvalidPoolConfig     = errors.New("gostage: invalid pool config")
	ErrDuplicateSpawner      = errors.New("gostage: duplicate spawner name")
	ErrInvalidSpawnerConfig  = errors.New("gostage: invalid spawner config")
	ErrUnknownSpawner        = errors.New("gostage: unknown spawner")
	ErrChildMetadataConflict = errors.New("gostage: child metadata conflict")
	ErrInvalidTLSConfig      = errors.New("gostage: invalid TLS configuration")
)

// ValidateConfig performs static checks across bootstrap options.
func ValidateConfig(cfg *Config) error {
	if err := validateSpawners(cfg.Spawners); err != nil {
		return err
	}
	if err := validatePools(cfg.Pools, cfg.Spawners); err != nil {
		return err
	}
	return nil
}

func validateSpawners(spawners []SpawnerConfig) error {
	seen := make(map[string]struct{}, len(spawners))
	for _, sp := range spawners {
		if sp.Name == "" || sp.BinaryPath == "" {
			return errors.Join(ErrInvalidSpawnerConfig, fmt.Errorf("spawner %q requires name and binary path", sp.Name))
		}
		if _, ok := seen[sp.Name]; ok {
			return errors.Join(ErrDuplicateSpawner, fmt.Errorf("spawner %q defined multiple times", sp.Name))
		}
		seen[sp.Name] = struct{}{}
		if err := validateTLSFiles(sp.TLS); err != nil {
			return err
		}
	}
	return nil
}

func validatePools(pools []PoolConfig, spawners []SpawnerConfig) error {
	if len(pools) == 0 {
		return nil
	}
	spawnerIndex := make(map[string]struct{}, len(spawners))
	for _, sp := range spawners {
		spawnerIndex[sp.Name] = struct{}{}
	}
	seenNames := make(map[string]struct{}, len(pools))
	for idx, pool := range pools {
		name := pool.Name
		if name == "" {
			name = fmt.Sprintf("pool-%d", idx+1)
		}
		if _, ok := seenNames[name]; ok {
			return errors.Join(ErrDuplicatePool, fmt.Errorf("pool %q defined multiple times", name))
		}
		seenNames[name] = struct{}{}
		if pool.Slots <= 0 {
			return errors.Join(ErrInvalidPoolConfig, fmt.Errorf("pool %q must specify Slots > 0", name))
		}
		if pool.Spawner != "" {
			if _, ok := spawnerIndex[pool.Spawner]; !ok {
				return errors.Join(ErrUnknownSpawner, fmt.Errorf("pool %q references unknown spawner %q", name, pool.Spawner))
			}
		}
	}
	return nil
}

func validateTLSFiles(files TLSFiles) error {
	// If all empty, ok.
	if files.CertPath == "" && files.KeyPath == "" && files.CAPath == "" {
		return nil
	}
	if files.CertPath == "" || files.KeyPath == "" {
		return errors.Join(ErrInvalidTLSConfig, fmt.Errorf("tls requires cert and key path"))
	}
	return nil
}
