package main

import (
	"bytes"
	"fmt"
	"go/format"
	"os"
	"os/exec"
)

func main() {
	command := exec.Command(
		"go",
		"tool",
		"cgo",
		"-godefs",
		"--",
		"-Iinternal/testdata/liburingoracle/include",
		"uapi_linux.go",
	)
	output, err := command.CombinedOutput()
	if err != nil {
		fmt.Fprintf(os.Stderr, "generate io_uring UAPI: %v\n%s", err, output)
		os.Exit(1)
	}
	packageIndex := bytes.Index(output, []byte("package ringo"))
	if packageIndex < 0 {
		fmt.Fprintln(os.Stderr, "generate io_uring UAPI: missing package declaration")
		os.Exit(1)
	}
	source := append([]byte(
		"//go:build linux\n\n"+
			"// Code generated from the bundled Linux io_uring UAPI by cgo -godefs; DO NOT EDIT.\n\n",
	), output[packageIndex:]...)
	source, err = format.Source(source)
	if err != nil {
		fmt.Fprintf(os.Stderr, "format generated io_uring UAPI: %v\n%s", err, source)
		os.Exit(1)
	}
	if err := os.WriteFile("z_uapi.go", source, 0o644); err != nil {
		fmt.Fprintf(os.Stderr, "write generated io_uring UAPI: %v\n", err)
		os.Exit(1)
	}
}
