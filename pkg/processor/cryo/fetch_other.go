//go:build !unix

package cryo

import "os/exec"

// killProcessGroup leaves the default behaviour in place: platforms without
// process groups fall back to signalling the direct child only.
func killProcessGroup(_ *exec.Cmd) {}
