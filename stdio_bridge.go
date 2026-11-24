package main

import (
	"context"
	"errors"
	"net/url"
	"os"
	"strings"

	"github.com/mark3labs/mcp-go/mcp"
	"github.com/mark3labs/mcp-go/server"
	"github.com/tbxark/optional-go"
)

func serveGatewayStdio(route string) (err error) {
	if err := validateServerName(route); err != nil || strings.Contains(route, "/") {
		return errors.New("invalid gateway route")
	}
	token := os.Getenv("MCP_PROXY_TOKEN")
	if token == "" || strings.ContainsAny(token, " \t\r\n") {
		return errors.New("MCP_PROXY_TOKEN is missing or invalid")
	}
	endpoint := url.URL{Scheme: "http", Host: "127.0.0.1:19090", Path: "/" + route + "/mcp"}
	config := &MCPClientConfigV2{
		TransportType: MCPClientTypeStreamable,
		URL:           endpoint.String(),
		Headers:       map[string]string{"Authorization": "Bearer " + token},
		Options:       &OptionsV2{AutoReconnect: optional.NewField(true)},
	}
	client, err := newMCPClient(route, config)
	if err != nil {
		return err
	}
	defer func() {
		if closeErr := client.Close(); closeErr != nil {
			err = errors.Join(err, redactURLCredentials(closeErr))
		}
	}()
	srv, err := newMCPServer(route, &MCPProxyConfigV2{
		Type:    MCPServerTypeStreamable,
		Version: BuildVersion,
	}, config)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := client.addToMCPServer(ctx, mcp.Implementation{Name: "local-gateway-stdio", Version: BuildVersion}, srv.mcpServer); err != nil {
		return err
	}
	return server.ServeStdio(srv.mcpServer, gatewayStdioProtocol())
}

func gatewayStdioProtocol() server.StdioOption {
	// mcp-go v1.1.0 blocks its stdio input loop during a modern subscription.
	// Negotiate legacy MCP on this hop only until stdio can multiplex requests.
	return server.WithStdioContextFunc(func(ctx context.Context) context.Context {
		return server.WithSupportedProtocolVersions(ctx, mcp.LegacyProtocolVersions())
	})
}
