package redis

import (
	"bufio"
	"context"
	"net"
	"time"
)

// Options configures a Client.
type Options struct {
	// Addr is the Redis server address in host:port form.
	Addr string
}

// Client is a minimal Redis client.
type Client struct {
	opt *Options
	cn  net.Conn
	rd  *bufio.Reader
	wr  *bufio.Writer
}

// NewClient creates a Client and establishes a connection.
func NewClient(opt *Options) *Client {
	c := &Client{opt: opt}
	c.connect()
	return c
}

func (c *Client) connect() {
	if c.opt == nil || c.opt.Addr == "" {
		return
	}
	conn, err := net.DialTimeout("tcp", c.opt.Addr, 5*time.Second)
	if err != nil {
		return
	}
	c.cn = conn
	c.rd = bufio.NewReader(conn)
	c.wr = bufio.NewWriter(conn)
}

// Close closes the underlying connection.
func (c *Client) Close() error {
	if c.cn != nil {
		return c.cn.Close()
	}
	return nil
}

// Set sets key to value.
func (c *Client) Set(ctx context.Context, key string, value interface{}, expiration time.Duration) *SetCmd {
	cmd := newSetCmd(key, value)
	if expiration > 0 {
		cmd.cmdArgs = append(cmd.cmdArgs, "PX", itoa(int64(expiration/time.Millisecond)))
	}
	c.process(cmd)
	return cmd
}

// Get returns the value of key.
func (c *Client) Get(ctx context.Context, key string) *GetCmd {
	cmd := newGetCmd(key)
	c.process(cmd)
	return cmd
}

// LPush prepends values to the list stored at key.
func (c *Client) LPush(ctx context.Context, key string, values ...interface{}) *LPushCmd {
	cmd := newLPushCmd(key, values...)
	c.process(cmd)
	return cmd
}

// Del deletes keys.
func (c *Client) Del(ctx context.Context, keys ...string) *DelCmd {
	cmd := newDelCmd(keys...)
	c.process(cmd)
	return cmd
}

// process executes a single command and reads its reply.
func (c *Client) process(cmd Cmder) {
	if c.cn == nil {
		cmd.setErr(errNotConnected)
		return
	}
	if err := writeCommand(c.wr, cmd.args()...); err != nil {
		cmd.setErr(err)
		return
	}
	reply, err := readReply(c.rd)
	if err != nil {
		cmd.setErr(err)
		return
	}
	_ = cmd.readReply(reply)
}

// Pipeline returns a new non-transactional pipeline.
func (c *Client) Pipeline() *Pipeline {
	return &Pipeline{client: c, cmds: make([]Cmder, 0)}
}

// TxPipeline returns a new transactional pipeline.
func (c *Client) TxPipeline() *TxPipeline {
	return &TxPipeline{Pipeline: Pipeline{client: c, cmds: make([]Cmder, 0)}}
}

func itoa(n int64) string {
	if n == 0 {
		return "0"
	}
	neg := n < 0
	if neg {
		n = -n
	}
	var b [20]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		i--
		b[i] = '-'
	}
	return string(b[i:])
}
