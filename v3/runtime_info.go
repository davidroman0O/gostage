package gostage

import (
	"os"
	"path/filepath"
)

// CurrentBinary returns the absolute path to the currently running binary. It
// falls back to the raw executable path if symlink resolution fails.
func CurrentBinary() string {
	path, err := os.Executable()
	if err != nil {
		return ""
	}
	resolved, err := filepath.EvalSymlinks(path)
	if err == nil {
		return resolved
	}
	return path
}
