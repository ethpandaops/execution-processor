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
// cannot change the structure of the config. Mapping keys, comments and $
// characters not followed by { are left unchanged. A plain (unquoted) scalar
// is typed by its replaced value, so `port: ${PORT}` decodes into an int field.
//
// It returns an error naming every variable that is unset or empty and has no
// default.
func UnmarshalWithEnv(data []byte, out any) error {
	var doc yaml.Node

	if err := yaml.Unmarshal(data, &doc); err != nil {
		return err
	}

	if doc.Kind == 0 {
		return nil
	}

	var missing []string

	substituteEnvVars(&doc, &missing)

	if len(missing) > 0 {
		slices.Sort(missing)

		return fmt.Errorf("config references unset or empty environment variables: %s", strings.Join(slices.Compact(missing), ", "))
	}

	return doc.Decode(out)
}

func substituteEnvVars(node *yaml.Node, missing *[]string) {
	switch node.Kind {
	case yaml.ScalarNode:
		if !envVarPattern.MatchString(node.Value) {
			return
		}

		node.Value = envVarPattern.ReplaceAllStringFunc(node.Value, func(match string) string {
			parts := envVarPattern.FindStringSubmatch(match)

			value := os.Getenv(parts[1])
			if value != "" {
				return value
			}

			if !strings.Contains(match, ":-") {
				*missing = append(*missing, parts[1])
			}

			return parts[2]
		})

		if node.Style == 0 {
			node.Tag = ""
		}
	case yaml.MappingNode:
		for i := 1; i < len(node.Content); i += 2 {
			substituteEnvVars(node.Content[i], missing)
		}
	case yaml.DocumentNode, yaml.SequenceNode:
		for _, child := range node.Content {
			substituteEnvVars(child, missing)
		}
	}
}
