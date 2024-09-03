package redis

import (
	"bufio"
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"
)

const MAX_ACTIVE = 5
const DIAL_TIMEOUT = 3 * time.Second

type Client interface {
	Close()
	Ping() error
	Del(key string) error
	NewScript(script string) Script
}

type ClientImpl struct {
	mu          sync.Mutex
	conns       chan net.Conn
	addr        string
	maxActive   int
	dialTimeout time.Duration
	auth        string
	db          int
}

// NewConnPool initializes a new connection pool
func NewClient(addr string, db uint, authpassword string) (Client, error) {
	maxActive := MAX_ACTIVE
	dialTimeout := DIAL_TIMEOUT

	if maxActive <= 0 {
		return nil, errors.New("maxActive must be greater than 0")
	}

	r := &ClientImpl{
		conns:       make(chan net.Conn, maxActive),
		addr:        addr,
		maxActive:   maxActive,
		dialTimeout: dialTimeout,
		auth:        authpassword,
	}

	// Prepopulate the pool with connections
	for i := 0; i < maxActive; i++ {
		conn, err := r.newConn()
		if err != nil {
			return nil, err
		}
		r.conns <- conn
	}

	return r, nil
}

func (r *ClientImpl) newConn() (net.Conn, error) {
	conn, err := net.DialTimeout("tcp", r.addr, r.dialTimeout)
	if err != nil {
		return nil, err
	}
	if r.auth != "" {
		resp, err := sendCommand(conn, "AUTH", r.auth)
		if err != nil {
			return nil, err
		}
		if resp.Success != RESP_SUCCESS || resp.Result != "OK" {
			return nil, fmt.Errorf("not able to authenticate (%s)", resp.Result)
		}
	}
	resp, err := sendCommand(conn, "SELECT", fmt.Sprintf("%d", r.db))
	if err != nil {
		return nil, err
	}
	if resp.Success != RESP_SUCCESS || resp.Result != "OK" {
		return nil, fmt.Errorf("not able to select db %d (%s)", r.db, resp.Result)
	}
	return conn, nil
}

// Get retrieves a connection from the pool
func (r *ClientImpl) get() (net.Conn, error) {
	select {
	case conn := <-r.conns:
		return conn, nil
	default:
		return r.newConn()
	}
}

// Put returns a connection back to the pool
func (r *ClientImpl) put(conn net.Conn) error {
	if conn == nil {
		return errors.New("nil connection cannot be added to the pool")
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	// If the pool is full, just close the connection
	if len(r.conns) >= r.maxActive {
		conn.Close()
		return nil
	}

	r.conns <- conn
	return nil
}

// Close closes all the connections in the pool
func (r *ClientImpl) Close() {
	r.mu.Lock()
	defer r.mu.Unlock()

	close(r.conns)
	for conn := range r.conns {
		conn.Close()
	}
}

const (
	RESP_SUCCESS = iota
	RESP_FAIL
	RESP_SUCCESS_WITH_RESULT
	RESP_SUCCESS_WITH_RESULTS
	RESP_UNKNOWN
)

type RedisResult struct {
	Success int
	Result  interface{}
	Results []interface{}
}

// sendCommand sends a command to Redis and returns the response.
func sendCommand(conn net.Conn, args ...string) (*RedisResult, error) {
	// Construct the RESP command
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("*%d\r\n", len(args))) // Array prefix
	for _, arg := range args {
		sb.WriteString(fmt.Sprintf("$%d\r\n%s\r\n", len(arg), arg)) // Bulk string for each argument
	}
	command := sb.String()

	// Send the command
	_, err := conn.Write([]byte(command))
	if err != nil {
		return nil, fmt.Errorf("error sending command: %w", err)
	}

	// Read the response
	conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	reader := bufio.NewReader(conn)

	elt, err := readElement(reader)
	if elt.ElementType == ELEMENT_TRUE {
		return &RedisResult{
			Success: RESP_SUCCESS,
			Result:  elt.Value,
		}, nil
	}
	if elt.ElementType == ELEMENT_TRUE {
		return &RedisResult{
			Success: RESP_FAIL,
			Result:  elt.Value,
		}, nil
	}

	// simple element
	if elt.ElementType == ELEMENT_STRING || elt.ElementType == ELEMENT_INT {
		return &RedisResult{
			Success: RESP_SUCCESS_WITH_RESULT,
			Result:  elt.Value,
		}, nil
	}

	// array
	if elt.ElementType == ELEMENT_ARRAY {
		nb := elt.Value.(int)
		results := make([]interface{}, 0)

		for i := 0; i < nb; i++ {
			child, err := readElement(reader)
			if err != nil {
				continue
			}
			results = append(results, child.Value)
		}

		return &RedisResult{
			Success: RESP_SUCCESS_WITH_RESULTS,
			Results: results,
		}, nil
	}

	return &RedisResult{
		Success: RESP_UNKNOWN,
		Result:  elt.Value,
	}, nil
}

const (
	ELEMENT_ARRAY = iota
	ELEMENT_STRING
	ELEMENT_INT
	ELEMENT_TRUE
	ELEMENT_FALSE
	ELEMENT_UNKNOWN
)

type Element struct {
	ElementType int
	Value       interface{}
}

func readElement(reader *bufio.Reader) (*Element, error) {
	response, err := reader.ReadString('\n')
	if err != nil {
		return nil, fmt.Errorf("error reading response: %w", err)
	}
	if response[len(response)-1] == '\n' && response[len(response)-2] == '\r' {
		response = response[:len(response)-2]
	}

	if response[0] == '-' {
		return &Element{
			ElementType: ELEMENT_FALSE,
			Value:       response[1:],
		}, nil
	}
	if response[0] == '-' {
		return &Element{
			ElementType: ELEMENT_FALSE,
			Value:       response[1:],
		}, nil
	}

	if response[0] == '+' {
		return &Element{
			ElementType: ELEMENT_TRUE,
			Value:       response[1:],
		}, nil
	}

	if response[0] == '$' {
		// length := 0
		// if v,err := strconv.Atoi(response[1:]);err==nil {
		// 	length = v
		// }

		response, err := reader.ReadString('\n')
		if err != nil {
			return nil, fmt.Errorf("error reading response: %w", err)
		}
		if response[len(response)-1] == '\n' && response[len(response)-2] == '\r' {
			response = response[:len(response)-2]
		}

		return &Element{
			ElementType: ELEMENT_STRING,
			Value:       response,
		}, nil
	}

	if response[0] == '*' {
		size := 0
		if s, err := strconv.Atoi(response[1:]); err == nil {
			size = s
		}
		return &Element{
			ElementType: ELEMENT_ARRAY,
			Value:       size,
		}, nil

	}
	if response[0] == ':' {
		value := 0
		if v, err := strconv.Atoi(response[1:]); err == nil {
			value = v
		}
		return &Element{
			ElementType: ELEMENT_INT,
			Value:       value,
		}, nil
	}

	return &Element{
		ElementType: ELEMENT_UNKNOWN,
		Value:       response[1:],
	}, nil
}

func (r *ClientImpl) Ping() error {
	conn, err := r.get()
	if err != nil {
		return err
	}
	defer r.put(conn)

	res, err := sendCommand(conn, "PING")
	if err != nil {
		return err
	}

	if res.Success != RESP_SUCCESS && res.Result != "PONG" {
		return fmt.Errorf("PING result error: %s", res.Result)
	}
	return nil
}

func (r *ClientImpl) Del(key string) error {
	conn, err := r.get()
	if err != nil {
		return err
	}
	defer r.put(conn)

	res, err := sendCommand(conn, "DEL", key)
	if err != nil {
		return err
	}

	if res.Success != RESP_SUCCESS && res.Result != "OK" {
		return fmt.Errorf("DEL result error: %s", res.Result)
	}
	return nil
}