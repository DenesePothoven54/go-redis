package redis

import (
	"context"
	"errors"
	"io"
)

// Pipeline is a non-transactional command pipeline. Commands are queued and
// executed together on Exec. Each command retains its own result or error.
type Pipeline struct {
	client *Client
	cmds   []Cmder
}

// Set queues a SET command.
func (p *Pipeline) Set(ctx context.Context, key string, value interface{}, expiration interface{}) *SetCmd {
	cmd := newSetCmd(key, value)
	p.cmds = append(p.cmds, cmd)
	return cmd
}

// Get queues a GET command.
func (p *Pipeline) Get(ctx context.Context, key string) *GetCmd {
	cmd := newGetCmd(key)
	p.cmds = append(p.cmds, cmd)
	return cmd
}

// LPush queues an LPUSH command.
func (p *Pipeline) LPush(ctx context.Context, key string, values ...interface{}) *LPushCmd {
	cmd := newLPushCmd(key, values...)
	p.cmds = append(p.cmds, cmd)
	return cmd
}

// Del queues a DEL command.
func (p *Pipeline) Del(ctx context.Context, keys ...string) *DelCmd {
	cmd := newDelCmd(keys...)
	p.cmds = append(p.cmds, cmd)
	return cmd
}

// Exec writes all queued commands and reads their replies. It returns the
// first command error, if any. Redis-level errors are recorded only on the
// command that produced them; transport errors invalidate the whole pipeline.
func (p *Pipeline) Exec(ctx context.Context) ([]Cmder, error) {
	if p.client.cn == nil {
		for _, cmd := range p.cmds {
			cmd.setErr(errNotConnected)
		}
		return p.cmds, errNotConnected
	}

	// Write all commands.
	for _, cmd := range p.cmds {
		if err := writeCommand(p.client.wr, cmd.args()...); err != nil {
			p.failAll(err)
			return p.cmds, err
		}
	}

	// Read replies one per command.
	var firstErr error
	for i, cmd := range p.cmds {
		reply, err := readReply(p.client.rd)
		if err != nil {
			// Transport/protocol error: invalidate this and all remaining.
			p.failFrom(i, err)
			if firstErr == nil {
				firstErr = err
			}
			return p.cmds, firstErr
		}
		if re, ok := reply.(RedisError); ok {
			// Redis-level error: record only on this command.
			cmd.setErr(re)
			if firstErr == nil {
				firstErr = re
			}
			continue
		}
		if err := cmd.readReply(reply); err != nil {
			cmd.setErr(err)
			if firstErr == nil {
				firstErr = err
			}
		}
	}
	return p.cmds, firstErr
}

// failFrom sets err on cmd[i] and every subsequent command.
func (p *Pipeline) failFrom(i int, err error) {
	for ; i < len(p.cmds); i++ {
		p.cmds[i].setErr(err)
	}
}

// failAll sets err on every queued command.
func (p *Pipeline) failAll(err error) {
	for _, cmd := range p.cmds {
		cmd.setErr(err)
	}
}

// TxPipeline is a transactional pipeline using MULTI/EXEC.
type TxPipeline struct {
	Pipeline
}

// Exec wraps the queued commands in MULTI/EXEC and maps each EXEC reply to the
// corresponding command. Redis errors inside the EXEC array are recorded only
// on the command that produced them.
func (p *TxPipeline) Exec(ctx context.Context) ([]Cmder, error) {
	if p.client.cn == nil {
		for _, cmd := range p.cmds {
			cmd.setErr(errNotConnected)
		}
		return p.cmds, errNotConnected
	}

	// MULTI
	if err := writeCommand(p.client.wr, "MULTI"); err != nil {
		p.failAll(err)
		return p.cmds, err
	}
	if _, err := readReply(p.client.rd); err != nil {
		p.failAll(err)
		return p.cmds, err
	}

	// Queue commands.
	for _, cmd := range p.cmds {
		if err := writeCommand(p.client.wr, cmd.args()...); err != nil {
			p.failAll(err)
			return p.cmds, err
		}
		if _, err := readReply(p.client.rd); err != nil {
			p.failAll(err)
			return p.cmds, err
		}
	}

	// EXEC
	if err := writeCommand(p.client.wr, "EXEC"); err != nil {
		p.failAll(err)
		return p.cmds, err
	}
	reply, err := readReply(p.client.rd)
	if err != nil {
		p.failAll(err)
		return p.cmds, err
	}

	arr, ok := reply.([]interface{})
	if !ok {
		// EXEC returned a single error (e.g. transaction aborted).
		if re, isErr := reply.(RedisError); isErr {
			for _, cmd := range p.cmds {
				cmd.setErr(re)
			}
			return p.cmds, re
		}
		p.failAll(errors.New("redis: unexpected EXEC reply"))
		return p.cmds, errors.New("redis: unexpected EXEC reply")
	}

	var firstErr error
	for i, cmd := range p.cmds {
		if i >= len(arr) {
			break
		}
		item := arr[i]
		if re, isErr := item.(RedisError); isErr {
			cmd.setErr(re)
			if firstErr == nil {
				firstErr = re
			}
			continue
		}
		if err := cmd.readReply(item); err != nil {
			cmd.setErr(err)
			if firstErr == nil {
				firstErr = err
			}
		}
	}
	return p.cmds, firstErr
}

var _ = io.EOF
