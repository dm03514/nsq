package statsd


import (
	"errors"
	"fmt"
	"net"
	"time"
)

type DataDogClient struct {
	conn   net.Conn
	addr   string
	prefix string
}


type DataDogTags map[string]string


func NewDataDogClient(addr string, prefix string) *DataDogClient {
	return &DataDogClient{
		addr:   addr,
		prefix: prefix,
	}
}

func (c *DataDogClient) String() string {
	return c.addr
}

func (c *DataDogClient) CreateSocket() error {
	conn, err := net.DialTimeout("udp", c.addr, time.Second)
	if err != nil {
		return err
	}
	c.conn = conn
	return nil
}

func (c *DataDogClient) Close() error {
	return c.conn.Close()
}

func (c *DataDogClient) Incr(stat string, count int64, tags map[string]string) error {
	return c.send(stat, "%d|c", count, tags)
}

func (c *DataDogClient) Decr(stat string, count int64, tags map[string]string) error {
	return c.send(stat, "%d|c", -count, tags)
}

func (c *DataDogClient) Timing(stat string, delta int64, tags map[string]string) error {
	return c.send(stat, "%d|ms", delta, tags)
}

func (c *DataDogClient) Gauge(stat string, value int64, tags map[string]string) error {
	return c.send(stat, "%d|g", value, tags)
}

func (c *DataDogClient) send(stat string, format string, value int64, tags map[string]string) error {
	if c.conn == nil {
		return errors.New("not connected")
	}
	format = fmt.Sprintf("%s%s:%s", c.prefix, stat, format)
	_, err := fmt.Fprintf(c.conn, format, value)
	return err
}
