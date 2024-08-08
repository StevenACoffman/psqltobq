package main

import (
	"fmt"
	"github.com/StevenACoffman/anotherr/errors"
	"github.com/StevenACoffman/psqltobq/cmd"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"os"
	"runtime/debug"

	stackdriver "github.com/tommy351/zap-stackdriver"
	"go.uber.org/automaxprocs/maxprocs"
)

const (
	// exitFail is the exit code if the program
	// fails.
	exitFail = 1
	// exitSuccess is the exit code if the program succeeds.
	exitSuccess = 0
)

// https://pace.dev/blog/2020/02/12/why-you-shouldnt-use-func-main-in-golang-by-mat-ryer
func main() {
	gitCommit, err := getGitBuildVersion()
	if err != nil {
		fmt.Println("Unable to get build git commit at runtime")
		os.Exit(exitFail)
	}

	var level zap.AtomicLevel
	if os.Getenv("DEBUG") != "" {
		level = zap.NewAtomicLevelAt(zapcore.DebugLevel)
	} else {
		level = zap.NewAtomicLevelAt(zapcore.InfoLevel)
	}
	config := &zap.Config{
		Level:            level,
		Encoding:         "json",
		EncoderConfig:    stackdriver.EncoderConfig,
		OutputPaths:      []string{"stdout"},
		ErrorOutputPaths: []string{"stderr"},
	}
	l, err := config.Build(zap.WrapCore(func(core zapcore.Core) zapcore.Core {
		return &stackdriver.Core{
			Core: core,
		}
	}), zap.Fields(
		stackdriver.LogServiceContext(&stackdriver.ServiceContext{
			Service: "psqltobq",
			Version: gitCommit,
		}),
	))
	if err != nil {
		panic(err)
	}
	// set GOMAXPROCS based on container limits
	undo, err := maxprocs.Set()
	defer undo()
	if err != nil {
		l.Fatal("failed to set GOMAXPROCS:", zap.Error(err))
	}
	if err != nil {
		panic(err)
	}
	// pass all arguments without the executable name
	if err := cmd.Incremental(l); err != nil {
		l.Error(fmt.Sprintf("%+v\n", err), zap.Error(err))
		os.Exit(exitFail)
	}
	l.Info("Successful completion")
	os.Exit(exitSuccess)
}

func getGitBuildVersion() (string, error) {
	bi, ok := debug.ReadBuildInfo()
	if !ok || bi == nil {
		return "", errors.New("Build info not found")
	}
	for _, setting := range bi.Settings {
		if setting.Key == "vcs.revision" {
			return setting.Value, nil
		}
	}
	return "", errors.New("Build info not found")
}
