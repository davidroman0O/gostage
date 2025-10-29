package orchestrator

func copyMap(src map[string]any) map[string]any {
	if len(src) == 0 {
		return nil
	}
	dst := make(map[string]any, len(src))
	for k, v := range src {
		dst[k] = v
	}
	return dst
}

func copyBoolMap(src map[string]bool) map[string]bool {
	if len(src) == 0 {
		return nil
	}
	dst := make(map[string]bool, len(src))
	for k, v := range src {
		dst[k] = v
	}
	return dst
}

func copyStringMap(src map[string]string) map[string]string {
	if len(src) == 0 {
		return nil
	}
	dst := make(map[string]string, len(src))
	for k, v := range src {
		dst[k] = v
	}
	return dst
}

func copySpawnerConfig(cfg SpawnerConfig) SpawnerConfig {
	copyCfg := cfg
	if len(cfg.Args) > 0 {
		copyCfg.Args = append([]string(nil), cfg.Args...)
	}
	if len(cfg.Env) > 0 {
		copyCfg.Env = append([]string(nil), cfg.Env...)
	}
	if len(cfg.Tags) > 0 {
		copyCfg.Tags = append([]string(nil), cfg.Tags...)
	}
	if len(cfg.Metadata) > 0 {
		meta := make(map[string]string, len(cfg.Metadata))
		for k, v := range cfg.Metadata {
			meta[k] = v
		}
		copyCfg.Metadata = meta
	}
	return copyCfg
}

func appendUniqueStrings(dst []string, values ...string) []string {
	seen := make(map[string]struct{}, len(dst))
	for _, v := range dst {
		seen[v] = struct{}{}
	}
	for _, v := range values {
		if v == "" {
			continue
		}
		if _, exists := seen[v]; exists {
			continue
		}
		dst = append(dst, v)
		seen[v] = struct{}{}
	}
	return dst
}
