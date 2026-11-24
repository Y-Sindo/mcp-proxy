package main

import (
	"bytes"
	"encoding/json"
	"slices"
	"strings"
	"testing"

	"github.com/mark3labs/mcp-go/mcp"
	"github.com/mark3labs/mcp-go/server"
)

func TestGatewayStdioAdvertisesLegacyProtocols(t *testing.T) {
	stdio := server.NewStdioServer(server.NewMCPServer("gateway", "test"))
	gatewayStdioProtocol()(stdio)

	input := strings.NewReader(`{"jsonrpc":"2.0","id":1,"method":"server/discover","params":{"_meta":{` +
		`"` + mcp.MetaKeyProtocolVersion + `":"` + mcp.ProtocolVersion20260728 + `",` +
		`"` + mcp.MetaKeyClientCapabilities + `":{},` +
		`"` + mcp.MetaKeyClientInfo + `":{"name":"test","version":"1"}}}}` + "\n")
	var output bytes.Buffer
	if err := stdio.Listen(t.Context(), input, &output); err != nil {
		t.Fatal(err)
	}
	var response struct {
		Result mcp.DiscoverResult `json:"result"`
	}
	if err := json.Unmarshal(output.Bytes(), &response); err != nil {
		t.Fatalf("decode discovery response: %v; output: %s", err, &output)
	}
	if got, want := response.Result.SupportedVersions, mcp.LegacyProtocolVersions(); !slices.Equal(got, want) {
		t.Fatalf("stdio advertised %v; want %v; output: %s", got, want, &output)
	}
}
