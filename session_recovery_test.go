package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mark3labs/mcp-go/client"
	"github.com/mark3labs/mcp-go/mcp"
)

func TestSessionRecoveryStdioHelper(t *testing.T) {
	marker := os.Getenv("MCP_TEST_SESSION_MARKER")
	if marker == "" {
		return
	}
	_, statErr := os.Stat(marker)
	fresh := statErr == nil
	if statErr != nil && !os.IsNotExist(statErr) {
		os.Exit(1)
	}
	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		var req struct {
			ID     json.RawMessage `json:"id"`
			Method string          `json:"method"`
		}
		if json.Unmarshal(scanner.Bytes(), &req) != nil {
			os.Exit(1)
		}
		if req.ID == nil {
			continue
		}
		response := map[string]any{"jsonrpc": "2.0", "id": req.ID}
		var result any
		switch req.Method {
		case "initialize":
			if err := os.WriteFile(marker, []byte("initialized"), 0600); err != nil {
				os.Exit(1)
			}
			result = map[string]any{
				"protocolVersion": "2025-11-25", "capabilities": map[string]any{},
				"serverInfo": map[string]string{"name": "agency-like", "version": "1"},
			}
		case "tools/list":
			result = map[string]any{"tools": []any{map[string]any{
				"name": "write", "inputSchema": map[string]any{"type": "object"},
			}}}
		case "ping":
			result = map[string]any{}
		case "tools/call":
			file, err := os.OpenFile(marker+".calls", os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
			if err != nil {
				os.Exit(1)
			}
			_, err = file.WriteString("x")
			closeErr := file.Close()
			if err != nil || closeErr != nil {
				os.Exit(1)
			}
			if !fresh {
				response["error"] = map[string]any{"code": -32001, "message": "Session not found"}
			} else {
				result = map[string]any{"content": []any{}}
			}
		default:
			response["error"] = map[string]any{"code": -32601, "message": "unsupported"}
		}
		if result != nil {
			response["result"] = result
		}
		if json.NewEncoder(os.Stdout).Encode(response) != nil {
			os.Exit(1)
		}
	}
	os.Exit(0)
}

func TestSessionRecoveryRebuildsStdioDespiteHealthyPing(t *testing.T) {
	t.Parallel()
	marker := filepath.Join(t.TempDir(), "session")
	var options OptionsV2
	if err := json.Unmarshal([]byte(`{"autoReconnect":true,"pingInterval":"1h"}`), &options); err != nil {
		t.Fatal(err)
	}
	c, err := newMCPClient("agency-like", &MCPClientConfigV2{
		Command: os.Args[0], Args: []string{"-test.run=^TestSessionRecoveryStdioHelper$"},
		Env: map[string]string{"MCP_TEST_SESSION_MARKER": marker}, Options: &options,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close() }()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	if err := c.addToMCPServer(ctx, mcp.Implementation{Name: "gateway", Version: "1"}, newProxyServerForTest(t)); err != nil {
		t.Fatal(err)
	}
	if err := c.probe(ctx); err != nil {
		t.Fatalf("subprocess ping should be healthy: %v", err)
	}
	old := c.getClient()
	request := mcp.CallToolRequest{}
	request.Params.Name = "write"
	if _, err := c.callTool(ctx, request); !isSessionFailure(err) {
		t.Fatalf("expected stale remote session error, got %v", err)
	}
	deadline := time.Now().Add(3 * time.Second)
	for c.getClient() == old || c.hasInvalidSession() {
		if time.Now().After(deadline) {
			t.Fatal("stdio subprocess was not rebuilt")
		}
		time.Sleep(time.Millisecond)
	}
	calls, err := os.ReadFile(marker + ".calls")
	if err != nil || string(calls) != "x" {
		t.Fatalf("original request was replayed: calls=%q err=%v", calls, err)
	}
	if _, err := c.callTool(ctx, request); err != nil {
		t.Fatalf("explicit request on rebuilt subprocess failed: %v", err)
	}
}

func TestSessionFailureClassification(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		err  error
		want bool
	}{
		{nil, false},
		{errors.New("Session not found"), true},
		{fmt.Errorf("wrapped: %w", errors.New("Session not found")), true},
		{(&mcp.JSONRPCErrorDetails{Code: -32001, Message: "Session not found"}).AsError(), true},
		{mcp.ErrMethodNotFound, false},
		{errors.New("Resource not found"), false},
		{errors.New("Session not found in customer data"), false},
		{errors.New("HTTP 404"), false},
	} {
		if got := isSessionFailure(tt.err); got != tt.want {
			t.Errorf("isSessionFailure(%v) = %v, want %v", tt.err, got, tt.want)
		}
	}
}

func TestSessionFailureCoalescesAndIgnoresOldConnection(t *testing.T) {
	t.Parallel()
	old, live := &client.Client{}, &client.Client{}
	c := &Client{client: live, sessionFailure: make(chan struct{}, 1)}
	c.observeSessionFailure(old, errors.New("Session not found"))
	if c.hasInvalidSession() {
		t.Fatal("an old connection invalidated its replacement")
	}
	var workers sync.WaitGroup
	for range 32 {
		workers.Go(func() { c.observeSessionFailure(live, errors.New("Session not found")) })
	}
	workers.Wait()
	c.markProbeHealthy()
	if c.Health() != healthFailed || len(c.sessionFailure) != 1 {
		t.Fatal("session failure was not coalesced or healthy ping hid it")
	}
}

func TestSessionRecoveryDoesNotReplayRequests(t *testing.T) {
	t.Parallel()
	for _, method := range []string{"tools/call", "prompts/get", "resources/read", "probe", "disabled", "retry"} {
		t.Run(method, func(t *testing.T) {
			t.Parallel()
			var initializes, calls atomic.Int32
			downstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodPost {
					w.WriteHeader(http.StatusMethodNotAllowed)
					return
				}
				var req struct {
					ID     json.RawMessage `json:"id"`
					Method string          `json:"method"`
				}
				if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
					t.Error(err)
					return
				}
				if req.ID == nil {
					w.WriteHeader(http.StatusAccepted)
					return
				}
				response := map[string]any{"jsonrpc": "2.0", "id": req.ID}
				var result any
				switch req.Method {
				case "initialize":
					generation := initializes.Add(1)
					if method == "retry" && generation == 2 {
						response["error"] = map[string]any{"code": -32001, "message": "temporary connect failure"}
						break
					}
					result = map[string]any{
						"protocolVersion": "2025-11-25", "capabilities": map[string]any{},
						"serverInfo": map[string]string{"name": "expired-session", "version": "1"},
					}
				case "tools/list":
					result = map[string]any{"tools": []any{map[string]any{
						"name": "write", "inputSchema": map[string]any{"type": "object"},
					}}}
				case "prompts/list":
					result = map[string]any{"prompts": []any{map[string]any{"name": "prompt"}}}
				case "resources/list":
					result = map[string]any{"resources": []any{}}
				case "resources/templates/list":
					result = map[string]any{"resourceTemplates": []any{}}
				case "ping", "tools/call", "prompts/get", "resources/read":
					calls.Add(1)
					if initializes.Load() == 1 {
						response["error"] = map[string]any{"code": -32001, "message": "Session not found"}
					} else {
						switch req.Method {
						case "tools/call":
							result = map[string]any{"content": []any{}}
						case "prompts/get":
							result = map[string]any{"messages": []any{}}
						case "resources/read":
							result = map[string]any{"contents": []any{}}
						default:
							result = map[string]any{}
						}
					}
				default:
					response["error"] = map[string]any{"code": -32601, "message": "unsupported"}
				}
				if result != nil {
					response["result"] = result
				}
				w.Header().Set("Content-Type", "application/json")
				if err := json.NewEncoder(w).Encode(response); err != nil {
					t.Error(err)
				}
			}))
			defer downstream.Close()
			var options OptionsV2
			if err := json.Unmarshal([]byte(`{"autoReconnect":true,"pingInterval":"1h"}`), &options); err != nil {
				t.Fatal(err)
			}
			if method == "disabled" {
				if err := json.Unmarshal([]byte(`{"autoReconnect":false}`), &options); err != nil {
					t.Fatal(err)
				}
			}
			if method == "retry" {
				options.PingInterval = Duration(100 * time.Millisecond)
			}
			c, err := newMCPClient("test", &MCPClientConfigV2{
				TransportType: MCPClientTypeStreamable, URL: downstream.URL, Options: &options,
			})
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = c.Close() }()
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			front := newProxyServerForTest(t)
			if err := c.addToMCPServer(ctx, mcp.Implementation{Name: "gateway", Version: "1"}, front); err != nil {
				t.Fatal(err)
			}
			invoke := func() error {
				switch method {
				case "tools/call", "disabled", "retry":
					req := mcp.CallToolRequest{}
					req.Params.Name = "write"
					_, err := c.callTool(ctx, req)
					return err
				case "prompts/get":
					reply := front.HandleMessage(ctx, []byte(`{"jsonrpc":"2.0","id":1,"method":"prompts/get","params":{"name":"prompt"}}`))
					raw, err := json.Marshal(reply)
					if err != nil {
						return err
					}
					var response struct {
						Error *mcp.JSONRPCErrorDetails `json:"error"`
					}
					if err := json.Unmarshal(raw, &response); err != nil {
						return err
					}
					if response.Error != nil {
						return response.Error.AsError()
					}
					return nil
				case "resources/read":
					req := mcp.ReadResourceRequest{}
					req.Params.URI = "test://resource"
					_, err := c.readResource(ctx, req)
					return err
				default:
					return c.probe(ctx)
				}
			}
			if err := invoke(); err == nil || !strings.Contains(err.Error(), "Session not found") {
				t.Fatalf("original error must be returned, got %v", err)
			}
			if method == "disabled" {
				time.Sleep(20 * time.Millisecond)
				c.markProbeHealthy()
				if initializes.Load() != 1 || c.Health() != healthFailed {
					t.Fatal("disabled reconnect must stay degraded without rebuilding")
				}
				return
			}
			wantInitializes := int32(2)
			if method == "retry" {
				wantInitializes = 3
			}
			deadline := time.Now().Add(3 * time.Second)
			for c.hasInvalidSession() || initializes.Load() < wantInitializes {
				if time.Now().After(deadline) {
					t.Fatal("did not recover immediately; ping interval is one hour")
				}
				time.Sleep(time.Millisecond)
			}
			if calls.Load() != 1 || initializes.Load() != wantInitializes || c.Health() != healthOK {
				t.Fatalf("calls=%d initializes=%d health=%v", calls.Load(), initializes.Load(), c.Health())
			}
			if err := invoke(); err != nil {
				t.Fatalf("next explicit request failed: %v", err)
			}
			if calls.Load() != 2 {
				t.Fatal("failed request was replayed")
			}
		})
	}
}
