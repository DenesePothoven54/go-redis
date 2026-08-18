package redis

import (
	"context"
	"reflect"
	"strings"
	"sync"

	"github.com/redis/go-redis/v9/internal/proto"
)

type Pipeliner interface {
	StatefulCmdable
	Do(ctx context.Context, args ...interface{}) *Cmd
	Process(ctx context.Context, cmd Cmder) error
	Close() error
	Discard() error
	Exec(ctx context.Context) ([]Cmder, error)
}

var _ Pipeliner = (*Pipeline)(nil)

type Pipeline struct {
	cmdable
	statefulCmdable

	ctx context.Context
	c   *baseClient

	mu   sync.Mutex
	cmds []Cmder
}

func (c *Pipeline) init() {
	c.cmdable = c.Process
	c.statefulCmdable = c.Process
}

func (c *Pipeline) Process(ctx context.Context, cmd Cmder) error {
	c.mu.Lock()
	c.cmds = append(c.cmds, cmd)
	c.mu.Unlock()
	return nil
}

func (c *Pipeline) Close() error {
	c.mu.Lock()
	c.cmds = nil
	c.mu.Unlock()
	return nil
}

func (c *Pipeline) Discard() error {
	c.mu.Lock()
	c.cmds = c.cmds[:0]
	c.mu.Unlock()
	return nil
}

func (c *Pipeline) Exec(ctx context.Context) ([]Cmder, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if len(c.cmds) == 0 {
		return nil, nil
	}

	cmds := c.cmds
	c.cmds = nil

	return cmds, c.c.processPipeline(ctx, cmds)
}

func (c *baseClient) processPipeline(ctx context.Context, cmds []Cmder) error {
	return c.hooks.processPipeline(ctx, cmds, c.pipelineExec)
}

func (c *baseClient) pipelineExec(ctx context.Context, cmds []Cmder) error {
	var firstErr error
	err := c.withConn(ctx, func(ctx context.Context, cn *pool.Conn) error {
		if err := c.writeCmds(ctx, cn, cmds); err != nil {
			return err
		}
		return c.readCmds(cn, cmds)
	})
	if err != nil {
		return err
	}
	return firstErr
}

func (c *baseClient) writeCmds(ctx context.Context, cn *pool.Conn, cmds []Cmder) error {
	return cn.WithWriter(ctx, c.opt.WriteTimeout, func(wr *proto.Writer) error {
		return writeCmds(wr, cmds)
	})
}

func writeCmds(wr *proto.Writer, cmds []Cmder) error {
	for _, cmd := range cmds {
		if err := wr.WriteCmd(cmd.Args()); err != nil {
			return err
		}
	}
	return nil
}

func (c *baseClient) readCmds(cn *pool.Conn, cmds []Cmder) error {
	var firstErr error
	for _, cmd := range cmds {
		err := cmd.readReply(cn.Reader)
		if err != nil {
			if firstErr == nil {
				firstErr = err
			}
			if isNetworkError(err) {
				break
			}
		}
	}
	return firstErr
}

func setCmdsErr(cmds []Cmder, err error) { 
	if isRedisError(err) {
		return
	}
	for _, cmd := range cmds {
		if cmd.Err() == nil {
			cmd.SetErr(err)
		}
	}
}

func isRedisError(err error) bool {
	if err == nil {
		return false
	}
	t := reflect.TypeOf(err)
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	if t.Name() != "RedisError" || !strings.HasSuffix(t.PkgPath(), "internal/proto") {
		return false
	}
	s := err.Error()
	return !strings.HasPrefix(s, "EXECABORT") && !strings.HasPrefix(s, "ERR EXEC") && !strings.HasPrefix(s, "ERR MULTI")
}