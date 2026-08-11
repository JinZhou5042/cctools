// Minimal direct-protocol DataVine Workflow IR adaptor.
package main

import (
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"strconv"
	"time"
)

const (
	magic          = uint32(0x44564331)
	version        = uint16(1)
	opAuth         = uint16(1)
	opSubmit       = uint16(20)
	opAppend       = uint16(21)
	opSeal         = uint16(22)
	opDescribe     = uint16(23)
	opCapabilities = uint16(26)
	opResult       = uint16(27)
	statusOK       = uint32(0)
	maxResponse    = 64 * 1024 * 1024
)

type client struct {
	connection net.Conn
	requestID  uint64
}

func requireCapabilities(c *client) error {
	body, err := c.exchange(opCapabilities, nil)
	if err != nil {
		return fmt.Errorf("runtime capability negotiation failed before mutation: %w", err)
	}
	var capabilities struct {
		SchemaVersions []string `json:"schema_versions"`
	}
	if err := json.Unmarshal(body, &capabilities); err != nil {
		return fmt.Errorf("invalid runtime capabilities: %w", err)
	}
	required := map[string]bool{
		"datavine.workflow/v1":       false,
		"datavine.workflow-delta/v1": false,
	}
	for _, schema := range capabilities.SchemaVersions {
		if _, known := required[schema]; known {
			required[schema] = true
		}
	}
	for schema, supported := range required {
		if !supported {
			return fmt.Errorf("runtime does not support %s; no mutation attempted", schema)
		}
	}
	return nil
}

type workflowInfo struct {
	WorkflowID      string `json:"workflow_id"`
	Digest          string `json:"digest"`
	Generation      uint64 `json:"generation"`
	EventID         uint64 `json:"event_id"`
	State           string `json:"state"`
	Tasks           uint64 `json:"tasks"`
	Data            uint64 `json:"data"`
	Edges           uint64 `json:"edges"`
	RequestedOutput uint64 `json:"requested_outputs"`
}

func connect(endpoint, token string) (*client, error) {
	parsed, err := url.Parse(endpoint)
	if err != nil || parsed.Scheme != "tcp" || parsed.Host == "" {
		return nil, fmt.Errorf("endpoint must use tcp://host:port")
	}
	connection, err := net.DialTimeout("tcp", parsed.Host, 10*time.Second)
	if err != nil {
		return nil, err
	}
	result := &client{connection: connection}
	if _, err = result.exchange(opAuth, []byte(token)); err != nil {
		connection.Close()
		return nil, err
	}
	return result, nil
}

func (c *client) exchange(opcode uint16, payload []byte) ([]byte, error) {
	c.requestID++
	header := make([]byte, 20)
	binary.BigEndian.PutUint32(header[0:4], magic)
	binary.BigEndian.PutUint16(header[4:6], version)
	binary.BigEndian.PutUint16(header[6:8], opcode)
	binary.BigEndian.PutUint32(header[8:12], uint32(len(payload)))
	binary.BigEndian.PutUint64(header[12:20], c.requestID)
	if _, err := c.connection.Write(append(header, payload...)); err != nil {
		return nil, err
	}
	response := make([]byte, 24)
	if _, err := io.ReadFull(c.connection, response); err != nil {
		return nil, err
	}
	size := binary.BigEndian.Uint32(response[12:16])
	if binary.BigEndian.Uint32(response[0:4]) != magic ||
		binary.BigEndian.Uint16(response[4:6]) != version ||
		binary.BigEndian.Uint16(response[6:8]) != opcode ||
		binary.BigEndian.Uint64(response[16:24]) != c.requestID || size > maxResponse {
		return nil, fmt.Errorf("invalid native response header")
	}
	body := make([]byte, size)
	if _, err := io.ReadFull(c.connection, body); err != nil {
		return nil, err
	}
	status := binary.BigEndian.Uint32(response[8:12])
	if status != statusOK {
		return nil, fmt.Errorf("native RPC opcode %d returned status %d", opcode, status)
	}
	return body, nil
}

func decodeInfo(body []byte) (workflowInfo, error) {
	if len(body) < 104 || string(body[:4]) != "DWI1" {
		return workflowInfo{}, fmt.Errorf("invalid workflow info")
	}
	idSize := int(binary.BigEndian.Uint16(body[60:62]))
	if len(body) != 104+idSize {
		return workflowInfo{}, fmt.Errorf("truncated workflow info")
	}
	states := map[uint32]string{1: "open", 2: "sealed", 3: "cancelled", 4: "running", 5: "completed", 6: "failed", 7: "running_open", 8: "open_quiescent"}
	return workflowInfo{
		WorkflowID:      string(body[104:]),
		Digest:          string(body[64:104]),
		Generation:      binary.BigEndian.Uint64(body[8:16]),
		EventID:         binary.BigEndian.Uint64(body[16:24]),
		State:           states[binary.BigEndian.Uint32(body[4:8])],
		Tasks:           binary.BigEndian.Uint64(body[24:32]),
		Data:            binary.BigEndian.Uint64(body[32:40]),
		Edges:           binary.BigEndian.Uint64(body[40:48]),
		RequestedOutput: binary.BigEndian.Uint64(body[48:56]),
	}, nil
}

func identifierPayload(identifier string) []byte {
	payload := make([]byte, 2+len(identifier))
	binary.BigEndian.PutUint16(payload[:2], uint16(len(identifier)))
	copy(payload[2:], identifier)
	return payload
}

func main() {
	if len(os.Args) < 5 {
		fmt.Fprintln(os.Stderr, "usage: datavine_workflow_go submit ENDPOINT TOKEN FILE | append ENDPOINT TOKEN ID GENERATION FILE | seal ENDPOINT TOKEN ID GENERATION | status ENDPOINT TOKEN ID | result ENDPOINT TOKEN ID DATA_ID")
		os.Exit(2)
	}
	operation, endpoint, token := os.Args[1], os.Args[2], os.Args[3]
	c, err := connect(endpoint, token)
	if err != nil {
		panic(err)
	}
	defer c.connection.Close()
	if err = requireCapabilities(c); err != nil {
		panic(err)
	}
	var output any
	switch operation {
	case "submit":
		document, readErr := os.ReadFile(os.Args[4])
		if readErr != nil {
			panic(readErr)
		}
		body, requestErr := c.exchange(opSubmit, document)
		if requestErr != nil {
			panic(requestErr)
		}
		output, err = decodeInfo(body)
	case "append":
		if len(os.Args) != 7 {
			panic("append requires ID GENERATION FILE")
		}
		generation, parseErr := strconv.ParseUint(os.Args[5], 10, 64)
		if parseErr != nil {
			panic(parseErr)
		}
		document, readErr := os.ReadFile(os.Args[6])
		if readErr != nil {
			panic(readErr)
		}
		identifier := os.Args[4]
		payload := make([]byte, 12+len(identifier)+len(document))
		binary.BigEndian.PutUint16(payload[:2], uint16(len(identifier)))
		binary.BigEndian.PutUint64(payload[4:12], generation)
		copy(payload[12:], identifier)
		copy(payload[12+len(identifier):], document)
		body, requestErr := c.exchange(opAppend, payload)
		if requestErr != nil {
			panic(requestErr)
		}
		output, err = decodeInfo(body)
	case "seal":
		if len(os.Args) != 6 {
			panic("seal requires ID GENERATION")
		}
		generation, parseErr := strconv.ParseUint(os.Args[5], 10, 64)
		if parseErr != nil {
			panic(parseErr)
		}
		identifier := os.Args[4]
		payload := make([]byte, 12+len(identifier))
		binary.BigEndian.PutUint16(payload[:2], uint16(len(identifier)))
		binary.BigEndian.PutUint64(payload[4:12], generation)
		copy(payload[12:], identifier)
		body, requestErr := c.exchange(opSeal, payload)
		if requestErr != nil {
			panic(requestErr)
		}
		output, err = decodeInfo(body)
	case "status":
		body, requestErr := c.exchange(opDescribe, identifierPayload(os.Args[4]))
		if requestErr != nil {
			panic(requestErr)
		}
		output, err = decodeInfo(body)
	case "result":
		if len(os.Args) != 6 {
			panic("result requires DATA_ID")
		}
		dataID, parseErr := strconv.ParseUint(os.Args[5], 10, 64)
		if parseErr != nil {
			panic(parseErr)
		}
		identifier := os.Args[4]
		payload := make([]byte, 12+len(identifier))
		binary.BigEndian.PutUint16(payload[:2], uint16(len(identifier)))
		binary.BigEndian.PutUint64(payload[4:12], dataID)
		copy(payload[12:], identifier)
		body, requestErr := c.exchange(opResult, payload)
		if requestErr != nil {
			panic(requestErr)
		}
		output = map[string]any{"workflow_id": identifier, "data_id": dataID, "base64": base64.StdEncoding.EncodeToString(body)}
	default:
		panic("unknown operation")
	}
	if err != nil {
		panic(err)
	}
	encoded, err := json.Marshal(output)
	if err != nil {
		panic(err)
	}
	fmt.Println(string(encoded))
}
