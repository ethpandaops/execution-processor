package config

import (
	"fmt"
	"os"
	"regexp"
	"slices"
	"strings"

	"gopkg.in/yaml.v3"
)

var envVarPattern = regexp.MustCompile(`\$\{([^}:]+)(?::-([^}]*))?\}`)

// UnmarshalWithEnv decodes YAML config into out, like yaml.Unmarshal, after
// replacing ${VAR} and ${VAR:-default} in scalar values with values from the
// environment.
//
// ${VAR:-default} uses default when VAR is unset or empty. ${VAR} requires VAR
// to be set and non-empty. Placeholders are replaced in parsed values, never in
// the YAML text, so a value is used exactly as set in the environment and
// cannot change the structure of the config. Mapping keys, comments, bare
// $VAR, $$ and an unclosed ${ are left unchanged. A plain (unquoted) scalar
// is typed by its replaced value, so `port: ${PORT}` decodes into an int field.
//
// It returns an error listing, in order of appearance, every variable that is
// unset or empty and has no default.
func UnmarshalWithEnv(data []byte, out any) error {
	var doc yaml.Node

	if err := yaml.Unmarshal(data, &doc); err != nil {
		return err
	}

	if doc.Kind == 0 {
		return nil
	}

	var missing []string

	expandNode(&doc, &missing)

	if len(missing) > 0 {
		return fmt.Errorf("config references unset or empty environment variables: %s", strings.Join(missing, ", "))
	}

	return doc.Decode(out)
}

func expandNode(n *yaml.Node, missing *[]string) {
	switch n.Kind {
	case yaml.ScalarNode:
		expanded := expandEnvVars(n.Value, missing)
		if expanded == n.Value {
			return
		}

		n.Value = expanded

		if n.Style == 0 {
			n.Tag = ""
		}
	case yaml.MappingNode:
		for i := 1; i < len(n.Content); i += 2 {
			expandNode(n.Content[i], missing)
		}
	case yaml.DocumentNode, yaml.SequenceNode:
		for _, c := range n.Content {
			expandNode(c, missing)
		}
	}
}

func expandEnvVars(value string, missing *[]string) string {
	return envVarPattern.ReplaceAllStringFunc(value, func(match string) string {
		parts := envVarPattern.FindStringSubmatchIndex(match)
		name := match[parts[2]:parts[3]]

		if v := os.Getenv(name); v != "" {
			return v
		}

		if parts[4] >= 0 {
			return match[parts[4]:parts[5]]
		}

		if !slices.Contains(*missing, name) {
			*missing = append(*missing, name)
		}

		return ""
	})
}
