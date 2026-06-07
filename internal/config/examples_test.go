package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestExamples_Parse(t *testing.T) {
	paths := []string{filepath.Join("..", "..", "configs", "collector.example.yaml")}

	examplesDir := filepath.Join("..", "..", "configs", "examples")
	entries, err := os.ReadDir(examplesDir)
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".yaml" {
			continue
		}
		paths = append(paths, filepath.Join(examplesDir, entry.Name()))
	}

	for _, path := range paths {
		t.Run(filepath.Base(path), func(t *testing.T) {
			if _, err := Load(path); err != nil {
				t.Fatalf("Load: %v", err)
			}
		})
	}
}
