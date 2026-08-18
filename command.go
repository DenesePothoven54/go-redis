package redis

import (
	"errors"
	"fmt"
)

// Cmder is the interface implemented by all command types.
type Cmder interface {
	// Err returns the error associated with this command.
	Err() error
	// setErr records an error on this command.
	setErr(err error)
	// args returns the RESP arguments for this command.
	args() []string
	// readReply parses a single reply into this command.
	readReply(reply interface{}) error
}

type baseCmd struct {
	err  error
	cmdArgs []string
}

func (c *baseCmd) Err() error { return c.err }

func (c *baseCmd) setErr(err error) { c.err = err }

func (c *baseCmd) args() []string { return c.cmdArgs }

// StatusCmd is the result of a command returning a simple status string.
type StatusCmd struct {
	baseCmd
	val string
}

func (c *StatusCmd) Val() string { return c.val }

func (c *StatusCmd) readReply(reply interface{}) error {
	switch v := reply.(type) {
	case string:
		c.val = v
		return nil
	case RedisError:
		c.err = v
		return nil
	default:
		return errors.New("redis: unexpected reply for status command")
	}
}

// StringCmd is the result of a command returning a bulk string.
type StringCmd struct {
	baseCmd
	val string
}

func (c *StringCmd) Val() string { return c.val }

func (c *StringCmd) readReply(reply interface{}) error {
	switch v := reply.(type) {
	case string:
		c.val = v
		return nil
	case nil:
		return nil
	case RedisError:
		c.err = v
		return nil
	default:
		return errors.New("redis: unexpected reply for string command")
	}
}

// IntCmd is the result of a command returning an integer.
type IntCmd struct {
	baseCmd
	val int64
}

func (c *IntCmd) Val() int64 { return c.val }

func (c *IntCmd) readReply(reply interface{}) error {
	switch v := reply.(type) {
	case int64:
		c.val = v
		return nil
	case RedisError:
		c.err = v
		return nil
	default:
		return errors.New("redis: unexpected reply for int command")
	}
}

// SetCmd is the result of a SET command.
type SetCmd struct{ StatusCmd }

// GetCmd is the result of a GET command.
type GetCmd struct{ StringCmd }

// LPushCmd is the result of an LPUSH command.
type LPushCmd struct{ IntCmd }

// DelCmd is the result of a DEL command.
type DelCmd struct{ IntCmd }

// newSetCmd builds a SET command.
func newSetCmd(key string, value interface{}, args ...string) *SetCmd {
	cmd := &SetCmd{}
	cmd.cmdArgs = append([]string{"SET", key, fmtValue(value)}, args...)
	return cmd
}

// newGetCmd builds a GET command.
func newGetCmd(key string) *GetCmd {
	cmd := &GetCmd{}
	cmd.cmdArgs = []string{"GET", key}
	return cmd
}

// newLPushCmd builds an LPUSH command.
func newLPushCmd(key string, values ...interface{}) *LPushCmd {
	cmd := &LPushCmd{}
	cmd.cmdArgs = []string{"LPUSH", key}
	for _, v := range values {
		cmd.cmdArgs = append(cmd.cmdArgs, fmtValue(v))
	}
	return cmd
}

// newDelCmd builds a DEL command.
func newDelCmd(keys ...string) *DelCmd {
	cmd := &DelCmd{}
	cmd.cmdArgs = append([]string{"DEL"}, keys...)
	return cmd
}

func fmtValue(v interface{}) string {
	switch t := v.(type) {
	case string:
		return t
	case []byte:
		return string(t)
	default:
		return fmt.Sprint(t)
	}
}
