//go:build !unix

package data

func ProcessAlive(pid int) bool { return false }
