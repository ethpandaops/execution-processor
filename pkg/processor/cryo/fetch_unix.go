//go:build unix

package cryo

import (
	"os/exec"
	"syscall"
)

// killProcessGroup makes cryo lead its own process group so that cancelling
// the command signals everything it spawned, not just the direct child.
func killProcessGroup(cmd *exec.Cmd) {
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Cancel = func() error { return syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL) }
}
