// Package buildinfo exposes process-constant release identity.
package buildinfo

import (
	"fmt"
	"runtime"
	"runtime/debug"
)

// These values are intentionally free of wall-clock build time so identical
// source, toolchain, and linker inputs can produce identical artifacts.
// Release builds set version and revision with -ldflags -X.
var (
	version  = "dev"
	revision = "unknown"
)

// Info is the immutable identity of a Coral binary.
type Info struct {
	Version   string
	Revision  string
	Modified  bool
	GoVersion string
}

// Current returns linker-provided release identity, supplemented by Go VCS
// metadata for local builds.
func Current() Info {
	info := Info{
		Version:   normalized(version, "dev"),
		Revision:  normalized(revision, "unknown"),
		GoVersion: runtime.Version(),
	}

	bi, ok := debug.ReadBuildInfo()
	if !ok {
		return info
	}
	if info.Version == "dev" && bi.Main.Version != "" && bi.Main.Version != "(devel)" {
		info.Version = bi.Main.Version
	}
	for _, setting := range bi.Settings {
		switch setting.Key {
		case "vcs.revision":
			if info.Revision == "unknown" && setting.Value != "" {
				info.Revision = setting.Value
			}
		case "vcs.modified":
			info.Modified = setting.Value == "true"
		}
	}
	return info
}

func normalized(value, fallback string) string {
	if value == "" {
		return fallback
	}
	return value
}

// String returns a stable, human-readable CLI representation.
func (i Info) String() string {
	modified := ""
	if i.Modified {
		modified = ", modified"
	}
	return fmt.Sprintf("coral version=%s revision=%s%s go=%s",
		normalized(i.Version, "dev"),
		normalized(i.Revision, "unknown"),
		modified,
		normalized(i.GoVersion, "unknown"),
	)
}
