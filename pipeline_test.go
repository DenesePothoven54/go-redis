package redis_test

import (
	"context"
	"strings"
	"testing"

	redis "github.com/DenesePothoven54/go-redis"
)

func TestPipelineErrorPropagation(t *testing.T) {
	ctx := context.Background()
	rdb := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})
	defer rdb.Close()

	// Setup initial key type
	err := rdb.Set(ctx, "string_key", "value", 0).Err()
	if err != nil {
		t.Fatalf("setup failed: %v", err)
	}
	defer rdb.Del(ctx, "string_key", "next_key")

	pipe := rdb.Pipeline()

	// Command 1: Should succeed
	cmd1 := pipe.Set(ctx, "next_key", "value2", 0)
	// Command 2: Should fail (WRONGTYPE Operation against a key holding the wrong kind of value)
	cmd2 := pipe.LPush(ctx, "string_key", "element")
	// Command 3: Should succeed
	cmd3 := pipe.Get(ctx, "next_key")

	_, err = pipe.Exec(ctx)
	// Exec itself returns the first error, which is expected
	if err == nil {
		t.Fatal("expected pipeline exec to return an error, got nil")
	}

	// Verify Command 1
	if err := cmd1.Err(); err != nil {
		t.Errorf("cmd1 failed unexpectedly: %v", err)
	}

	// Verify Command 2 (The failing command)
	if err := cmd2.Err(); err == nil {
		t.Error("expected cmd2 to fail, but got nil error")
	} else if !strings.Contains(err.Error(), "WRONGTYPE") {
		t.Errorf("expected WRONGTYPE error for cmd2, got: %v", err)
	}

	// Verify Command 3 (Should NOT be overwritten by cmd2's error)
	if err := cmd3.Err(); err != nil {
		t.Errorf("cmd3 error was incorrectly overwritten: %v", err)
	}
	if val := cmd3.Val(); val != "value2" {
		t.Errorf("expected cmd3 value to be 'value2', got: %q", val)
	}
}

func TestTxPipelineErrorMapping(t *testing.T) {
	ctx := context.Background()
	rdb := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})
	defer rdb.Close()

	if err := rdb.Set(ctx, "string_key", "value", 0).Err(); err != nil {
		t.Fatalf("setup failed: %v", err)
	}
	defer rdb.Del(ctx, "string_key", "next_key")

	pipe := rdb.TxPipeline()
	cmd1 := pipe.Set(ctx, "next_key", "value2", 0)
	cmd2 := pipe.LPush(ctx, "string_key", "element")
	cmd3 := pipe.Get(ctx, "next_key")

	_, err := pipe.Exec(ctx)
	if err == nil {
		t.Fatal("expected tx pipeline exec to return an error, got nil")
	}

	if err := cmd1.Err(); err != nil {
		t.Errorf("cmd1 failed unexpectedly: %v", err)
	}
	if err := cmd2.Err(); err == nil {
		t.Error("expected cmd2 to fail, but got nil error")
	} else if !strings.Contains(err.Error(), "WRONGTYPE") {
		t.Errorf("expected WRONGTYPE error for cmd2, got: %v", err)
	}
	if err := cmd3.Err(); err != nil {
		t.Errorf("cmd3 error was incorrectly overwritten: %v", err)
	}
	if val := cmd3.Val(); val != "value2" {
		t.Errorf("expected cmd3 value to be 'value2', got: %q", val)
	}
}
