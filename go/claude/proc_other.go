//go:build !unix

package claude

import "os/exec"

func setProcessGroup(*exec.Cmd) {}

func killTree(cmd *exec.Cmd) error {
	if cmd.Process == nil {
		return nil
	}
	return cmd.Process.Kill()
}
