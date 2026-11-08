package mcp

import (
	"github.com/cli/cli/v2/pkg/cmd/mcp/serve"
	"github.com/cli/cli/v2/pkg/cmdutil"
	"github.com/spf13/cobra"
)

// NewCmdMCP constructs the native MCP command family.
func NewCmdMCP(f *cmdutil.Factory) *cobra.Command {
	cmd := &cobra.Command{
		Use:     "mcp <command>",
		Short:   "Expose GitHub Actions through native MCP Events",
		GroupID: "actions",
	}
	cmdutil.EnableRepoOverride(cmd, f)
	cmd.AddCommand(serve.NewCmdServe(f))
	return cmd
}
