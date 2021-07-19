package client

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"strconv"
	"strings"
)

const (
	GET = "get"
	SET = "set"
	DEL = "del"
	TCPPort = 12346
)

type TcpClient struct {
	net.Conn
	r *bufio.Reader
}

func (c *TcpClient) SendGet(key string) error {
	log.Printf("req-GET: key=%s", key)
	if _, err := c.Write([]byte(fmt.Sprintf("G%d %s", len(key), key))); err != nil {
		return errors.New("failed to write get of ABNF: G<klen><SP><kcontent>, error: " + err.Error())
	}
	return nil
}

func (c *TcpClient) SendSet(key string, value []byte) error {
	log.Printf("req-SET: key=%s, value=%s", key, string(value))
	if _, err := c.Write([]byte(fmt.Sprintf("S%d %d %s%s", len(key), len(value), key, string(value)))); err != nil {
		return errors.New("failed to write set of ABNF: S<klen><SP><vlen><SP><kcontent><vcontent>, error: " + err.Error())
	}
	return nil
}

func (c *TcpClient) SendDel(key string) error {
	log.Printf("req-DEL: key=%s", key)
	if _, err := c.Write([]byte(fmt.Sprintf("D%d %s", len(key), key))); err != nil {
		return errors.New("failed to write delete of ABNF: D<klen><SP><kcontent>, error: " + err.Error())
	}
	return nil
}

type TcpErr struct {
	name string
}

func (e *TcpErr) Error() string {
	return ""
}

func readLen(r *bufio.Reader) (int, error) {
	tmp, err := r.ReadString(' ')
	if err != nil {
		return 0, errors.New("failed to read data of length value by call bufio.Reader.ReadString(), error: " + err.Error())
	}
	tmp = strings.TrimSpace(tmp)
	// response is error
	if strings.EqualFold(tmp, "-") {
		return 0, &TcpErr{}
	}
	// response is value(abnf:bytes-array)
	length, err := strconv.Atoi(tmp)
	if err != nil {
		return 0, errors.New("failed to convert length-value from string to int, error: " + err.Error())
	}

	return length, nil
}

func readABNFBytesArray(r *bufio.Reader) (string, error) {
	lengthStr, err := r.ReadString(' ')
	if err != nil {
		return "", err
	}
	length, err := strconv.Atoi(lengthStr)
	if err != nil {
		return "", errors.New(fmt.Sprintf("failed to convert string(value='%s') to int, error: %s", lengthStr, err.Error()))
	}

	content := make([]byte, length)
	if _, err := io.ReadFull(r, content); err != nil {
		return "", errors.New("failed to read content from bytes-array of abnf, error: " + err.Error())
	}

	return string(content), nil
}

// recvResp
// @description 解析tcp abnf回包：response = error | bytes-array; error = '-'<SP><bytes-array>
func (c *TcpClient) recvResp() (string, error) {
	vlen, err := readLen(c.r)
	if err != nil {
		// if tcp resp is err-content, then write it to resp-content & return
		if _, ok := err.(*TcpErr); ok {
			eContent, err := readABNFBytesArray(c.r)
			if err != nil {
				return "", err
			}
			return eContent, nil
		}
		return "", err
	}

	value := make([]byte, vlen)
	if _, err := io.ReadFull(c.r, value); err != nil {
		return "", errors.New("failed to read content from bytes-array of abnf, error: " + err.Error())
	}
	log.Printf("recv: vlen=%d, value=%s", vlen, value)
	return string(value), nil
}

func (c *TcpClient) Run(cmd *Cmd) {
	switch cmd.Name {
	case GET:
		if err := c.SendGet(cmd.Key); err != nil {
			log.Printf("[TCP-Cli]: failed to send get req(key=%s), error: %s", cmd.Key, err.Error())
		}
	case SET:
		if err := c.SendSet(cmd.Key, []byte(cmd.Value)); err != nil {
			log.Printf("[TCP-Cli]: failed to send set req(key=%s, value=%s), error: %s", cmd.Key, cmd.Value, err.Error())
		}
	case DEL:
		if err := c.SendDel(cmd.Key); err != nil {
			log.Printf("[TCP-Cli]: failed to send del req(key=%s), error: %s", cmd.Key, err.Error())
		}
	default:
		panic("unknown cmd name: " + cmd.Name)
	}
	cmd.Value, cmd.Error = c.recvResp()
}

func (c *TcpClient) PipelineRun(cmds []*Cmd) {
	log.Println("in PipelineRun()")
	if len(cmds) == 0 {
		return
	}
	for _, cmd := range cmds {
		switch cmd.Name {
		case GET:
			if err := c.SendGet(cmd.Key); err != nil {
				log.Printf("[pipeline]: failed to send get req(key=%s), error: %s", cmd.Key, err.Error())
			}
		case SET:
			if err := c.SendSet(cmd.Key, []byte(cmd.Value)); err != nil {
				log.Printf("[pipline]: failed to send set req(key=%s, value=%s), error: %s", cmd.Key, cmd.Value, err.Error())
			}
		case DEL:
			if err := c.SendDel(cmd.Key); err != nil {
				log.Printf("[pipeline]: failed to send del req(key=%s), error: %s", cmd.Key, err.Error())
			}
		default:
			log.Printf("[pipeline]: unknown cmd name: %s", cmd.Name)
		}
	}
	log.Println("ready to recv...")
	for _, cmd := range cmds {
		cmd.Value, cmd.Error = c.recvResp()
		log.Printf("Resp: value=%s, errInfo=%s", cmd.Value, cmd.Error.Error())
	}
}

func newTCPClient(server string) *TcpClient {
	conn, err := net.Dial("tcp", fmt.Sprintf("%s:%d", server, TCPPort))
	if err != nil {
		panic(err)
	}
	return &TcpClient{
		Conn: conn,
		r:    bufio.NewReader(conn),
	}
}
