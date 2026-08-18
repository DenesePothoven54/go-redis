package redis

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"strconv"
)

// RedisError is an error returned by the Redis server for a specific command.
type RedisError string

func (e RedisError) Error() string { return string(e) }

// IsRedisError reports whether err is a Redis-level command error.
func IsRedisError(err error) bool {
	var re RedisError
	return errors.As(err, &re)
}

// writeCommand encodes args as a RESP array and writes it to w.
func writeCommand(w *bufio.Writer, args ...string) error {
	if _, err := fmt.Fprintf(w, "*%d\r\n", len(args)); err != nil {
		return err
	}
	for _, arg := range args {
		if _, err := fmt.Fprintf(w, "$%d\r\n%s\r\n", len(arg), arg); err != nil {
			return err
		}
	}
	return w.Flush()
}

// readReply reads a single RESP reply from r. It returns a RedisError for
// server-side errors (so callers can distinguish them from transport errors).
func readReply(r *bufio.Reader) (interface{}, error) {
	line, err := readLine(r)
	if err != nil {
		return nil, err
	}
	if len(line) == 0 {
		return nil, io.EOF
	}
	switch line[0] {
	case '+':
		return string(line[1:]), nil
	case '-':
		return RedisError(line[1:]), nil
	case ':':
		n, err := strconv.ParseInt(string(line[1:]), 10, 64)
		if err != nil {
			return nil, err
		}
		return n, nil
	case '$':
		n, err := strconv.Atoi(string(line[1:]))
		if err != nil {
			return nil, err
		}
		if n == -1 {
			return nil, nil
		}
		buf := make([]byte, n+2)
		if _, err := io.ReadFull(r, buf); err != nil {
			return nil, err
		}
		return string(buf[:n]), nil
	case '*':
		n, err := strconv.Atoi(string(line[1:]))
		if err != nil {
			return nil, err
		}
		if n == -1 {
			return nil, nil
		}
		arr := make([]interface{}, n)
		for i := 0; i < n; i++ {
			arr[i], err = readReply(r)
			if err != nil {
				return nil, err
			}
		}
		return arr, nil
	default:
		return nil, fmt.Errorf("redis: unexpected reply prefix %q", line[0])
	}
}

func readLine(r *bufio.Reader) ([]byte, error) {
	line, err := r.ReadBytes('\n')
	if err != nil {
		return nil, err
	}
	if len(line) >= 2 && line[len(line)-2] == '\r' {
		line = line[:len(line)-2]
	}
	return line, nil
}

// errNotConnected is returned when no connection is established.
var errNotConnected = errors.New("redis: client is not connected")
