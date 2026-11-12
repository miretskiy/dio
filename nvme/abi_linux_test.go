//go:build linux && cgo

package nvme

import (
	"testing"

	"github.com/miretskiy/dio/v2/nvme/internal/testdata/nvmeoracle"
	"github.com/stretchr/testify/require"
)

func TestCommandMatchesLinuxNVMeABI(t *testing.T) {
	n := &Device{info: Info{NSID: 7, BlockSize: 512, SizeBytes: 8 << 40, MaxTransferBytes: 128 << 10}}
	n.offset, n.length = 4<<40, 1<<20
	require.NoError(t, n.validateRange())
	cmd, err := n.command(true, make([]byte, 4096), 8192, true)
	require.NoError(t, err)
	require.Equal(t, nvmeoracle.RW(7, ((4<<40)+8192)/512, 4096, 7|ForceUnitAccess), cmd.payload())
	require.Equal(t, uint32(UringCmdIO), nvmeoracle.IOCommand())
	require.Equal(t, uint32(UringCmdAdmin), nvmeoracle.AdminCommand())
	require.Equal(t, uint(ioctlNamespaceID), nvmeoracle.NamespaceIDIoctl())
	require.Equal(t, uint(ioctlAdminCommand), nvmeoracle.AdminIoctl())
}
