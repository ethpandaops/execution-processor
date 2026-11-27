package config

import (
	"os"
	"testing"

	"github.com/creasty/defaults"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

type envTestConfig struct {
	Logging  string            `yaml:"logging"`
	Password string            `yaml:"password"`
	Address  string            `yaml:"address"`
	Port     int               `yaml:"port"`
	Enabled  bool              `yaml:"enabled"`
	Headers  map[string]string `yaml:"headers"`
	Nodes    []string          `yaml:"nodes"`
}

func TestUnmarshalWithEnv(t *testing.T) {
	tests := []struct {
		name    string
		content string
		env     map[string]string
		want    envTestConfig
	}{
		{
			name:    "variables are replaced in values",
			content: "password: ${EP_TEST_PASSWORD}\naddress: \"redis://:${EP_TEST_PASSWORD}@${EP_TEST_HOST}:6379\"\nheaders:\n  authorization: Bearer ${EP_TEST_TOKEN}\nnodes:\n  - ${EP_TEST_HOST}\n",
			env:     map[string]string{"EP_TEST_PASSWORD": "s3cr$t", "EP_TEST_HOST": "redis", "EP_TEST_TOKEN": "abc"},
			want: envTestConfig{
				Password: "s3cr$t",
				Address:  "redis://:s3cr$t@redis:6379",
				Headers:  map[string]string{"authorization": "Bearer abc"},
				Nodes:    []string{"redis"},
			},
		},
		{
			name:    "unquoted placeholders are typed by their value",
			content: "port: ${EP_TEST_PORT}\nenabled: ${EP_TEST_ENABLED}\npassword: ${EP_TEST_PORT}\n",
			env:     map[string]string{"EP_TEST_PORT": "9000", "EP_TEST_ENABLED": "true"},
			want:    envTestConfig{Port: 9000, Enabled: true, Password: "9000"},
		},
		{
			name:    "default used when variable unset or empty",
			content: "address: ${EP_TEST_HOST:-localhost}\nport: ${EP_TEST_PORT:-6379}\npassword: \"${EP_TEST_PASSWORD:-}\"\n",
			env:     map[string]string{"EP_TEST_PORT": ""},
			want:    envTestConfig{Address: "localhost", Port: 6379},
		},
		{
			name:    "default ignored when variable set",
			content: "address: ${EP_TEST_HOST:-localhost}",
			env:     map[string]string{"EP_TEST_HOST": "redis"},
			want:    envTestConfig{Address: "redis"},
		},
		{
			name:    "keys and comments are left unchanged",
			content: "# password: ${EP_TEST_UNSET}\nheaders:\n  ${EP_TEST_UNSET}: value # ${EP_TEST_UNSET}\n",
			want:    envTestConfig{Headers: map[string]string{"${EP_TEST_UNSET}": "value"}},
		},
		{
			name:    "literal dollar signs are left unchanged",
			content: "password: pa$$word $EP_TEST_PASSWORD $ {EP_TEST_PASSWORD} ${}\n",
			env:     map[string]string{"EP_TEST_PASSWORD": "secret"},
			want:    envTestConfig{Password: "pa$$word $EP_TEST_PASSWORD $ {EP_TEST_PASSWORD} ${}"},
		},
		{
			name:    "placeholders in values are not expanded again",
			content: "password: ${EP_TEST_PASSWORD}\n",
			env:     map[string]string{"EP_TEST_PASSWORD": "${EP_TEST_UNSET}"},
			want:    envTestConfig{Password: "${EP_TEST_UNSET}"},
		},
		{
			name:    "aliased values are replaced once",
			content: "password: &pw ${EP_TEST_PASSWORD}\naddress: *pw\n",
			env:     map[string]string{"EP_TEST_PASSWORD": "a${EP_TEST_PASSWORD}"},
			want:    envTestConfig{Password: "a${EP_TEST_PASSWORD}", Address: "a${EP_TEST_PASSWORD}"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for k, v := range tt.env {
				t.Setenv(k, v)
			}

			var got envTestConfig

			require.NoError(t, UnmarshalWithEnv([]byte(tt.content), &got))
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestUnmarshalWithEnvValuesArriveUnchanged(t *testing.T) {
	value := "a\tb #c\nlogging: debug\n\"d'e\\n"

	t.Setenv("EP_TEST_PASSWORD", value)

	for _, content := range []string{
		"logging: info\npassword: ${EP_TEST_PASSWORD}\n",
		"logging: info\npassword: \"${EP_TEST_PASSWORD}\"\n",
		"logging: info\npassword: '${EP_TEST_PASSWORD}'\n",
		"logging: info\npassword: |-\n  ${EP_TEST_PASSWORD}\n",
		"{logging: info, password: \"${EP_TEST_PASSWORD}\"}\n",
	} {
		var got envTestConfig

		require.NoError(t, UnmarshalWithEnv([]byte(content), &got), content)
		assert.Equal(t, envTestConfig{Logging: "info", Password: value}, got, content)
	}
}

func TestUnmarshalWithEnvUnsetOrEmptyVariable(t *testing.T) {
	t.Setenv("EP_TEST_SET", "value")
	t.Setenv("EP_TEST_EMPTY", "")

	content := "password: ${EP_TEST_UNSET_B}\naddress: ${EP_TEST_SET}\nnodes:\n  - ${EP_TEST_UNSET_A} ${EP_TEST_UNSET_B}\n  - \"${EP_TEST_EMPTY}\"\n"

	var got envTestConfig

	err := UnmarshalWithEnv([]byte(content), &got)
	require.EqualError(t, err, "config references unset or empty environment variables: EP_TEST_EMPTY, EP_TEST_UNSET_A, EP_TEST_UNSET_B")
	assert.Equal(t, envTestConfig{}, got)
}

func TestUnmarshalWithEnvMatchesYAMLWithoutPlaceholders(t *testing.T) {
	for _, content := range []string{"", "# comment only\n", "logging: debug\nport: 6379\nenabled: yes\npassword: ~\n"} {
		var want, got envTestConfig

		require.NoError(t, yaml.Unmarshal([]byte(content), &want))
		require.NoError(t, UnmarshalWithEnv([]byte(content), &got))
		assert.Equal(t, want, got, content)
	}

	data, err := os.ReadFile("../../example_config.yaml")
	require.NoError(t, err)

	want, got := &Config{}, &Config{}
	require.NoError(t, defaults.Set(want))
	require.NoError(t, defaults.Set(got))

	require.NoError(t, yaml.Unmarshal(data, want))
	require.NoError(t, UnmarshalWithEnv(data, got))
	assert.Equal(t, want, got)
}
