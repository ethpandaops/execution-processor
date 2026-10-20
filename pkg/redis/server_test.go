package redis

import (
	"context"
	"net"
	"strings"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/alicebob/miniredis/v2/server"
	"github.com/hibiken/asynq"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

const testPassword = "secret"

// newFakeSentinel serves the SENTINEL commands go-redis needs and always names master as the master.
func newFakeSentinel(t *testing.T, master *miniredis.Miniredis) *miniredis.Miniredis {
	t.Helper()

	sentinel := miniredis.RunT(t)
	sentinel.RequireAuth(testPassword)

	host, port, err := net.SplitHostPort(master.Addr())
	require.NoError(t, err)

	err = sentinel.Server().Register("SENTINEL", func(c *server.Peer, _ string, args []string) {
		if len(args) == 0 {
			c.WriteError("ERR wrong number of arguments for 'sentinel' command")

			return
		}

		switch strings.ToLower(args[0]) {
		case "get-master-addr-by-name":
			c.WriteLen(2)
			c.WriteBulk(host)
			c.WriteBulk(port)
		default:
			c.WriteLen(0)
		}
	})
	require.NoError(t, err)

	return sentinel
}

func TestNewWithSentinelUsesMaster(t *testing.T) {
	master := miniredis.RunT(t)
	master.RequireAuth(testPassword)

	sentinel := newFakeSentinel(t, master)

	client, err := New(&Config{
		Address: "redis://:" + testPassword + "@unused:6379/0",
		Sentinel: &SentinelConfig{
			MasterName: "mymaster",
			Addrs:      []string{sentinel.Addr()},
		},
	})
	require.NoError(t, err)

	defer client.Close()

	ctx := context.Background()

	require.NoError(t, client.Set(ctx, "key", "value", 0).Err())

	got, err := master.Get("key")
	require.NoError(t, err)
	require.Equal(t, "value", got)

	asynqClient := asynq.NewClient(ConnOpt(client))
	defer asynqClient.Close()

	_, err = asynqClient.Enqueue(asynq.NewTask("test", nil), asynq.Queue("q"))
	require.NoError(t, err)

	inspector := asynq.NewInspector(ConnOpt(client))
	defer inspector.Close()

	info, err := inspector.GetQueueInfo("q")
	require.NoError(t, err)
	require.Equal(t, 1, info.Pending)
	require.True(t, master.Exists("asynq:{q}:pending"))
}

func TestConnOptKeepsCredentialsAndDB(t *testing.T) {
	master := miniredis.RunT(t)
	master.RequireAuth(testPassword)

	client, err := New(&Config{Address: "redis://:" + testPassword + "@" + master.Addr() + "/3"})
	require.NoError(t, err)

	defer client.Close()

	made, ok := ConnOpt(client).MakeRedisClient().(*redis.Client)
	require.True(t, ok)

	defer made.Close()

	require.NoError(t, made.Set(context.Background(), "key", "value", 0).Err())

	master.Select(3)

	got, err := master.Get("key")
	require.NoError(t, err)
	require.Equal(t, "value", got)
}

func TestConfigValidateSentinel(t *testing.T) {
	tests := []struct {
		name     string
		sentinel *SentinelConfig
		wantErr  bool
	}{
		{name: "no sentinel", sentinel: nil},
		{name: "valid", sentinel: &SentinelConfig{MasterName: "mymaster", Addrs: []string{"sentinel:26379"}}},
		{name: "missing master name", sentinel: &SentinelConfig{Addrs: []string{"sentinel:26379"}}, wantErr: true},
		{name: "missing addrs", sentinel: &SentinelConfig{MasterName: "mymaster"}, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := &Config{Address: "redis://localhost:6379", Sentinel: tt.sentinel}

			err := cfg.Validate()
			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
		})
	}
}
