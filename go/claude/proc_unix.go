//go:build unix

// The line above is a build constraint: this file is only compiled on Unix
// systems (macOS, Linux, ...). proc_other.go covers everything else.

package claude

import (
	"os/exec"
	"syscall"
)

// setProcessGroup starts the command in its own process group, so killTree
// can stop it together with any children it started. Otherwise a child that
// inherited our stdout pipe would keep it open and our reader would hang.
func setProcessGroup(cmd *exec.Cmd) {
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
}

// killTree kills the whole process group (a negative pid means "the group").
func killTree(cmd *exec.Cmd) error {
	if cmd.Process == nil {
		return nil
	}
	return syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
}
