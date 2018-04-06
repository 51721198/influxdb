package export

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"runtime"
	"runtime/pprof"
	"time"

	"github.com/influxdata/influxdb/cmd/influx-tools/internal/format"
	"github.com/influxdata/influxdb/cmd/influx-tools/internal/format/binary"
	"github.com/influxdata/influxdb/cmd/influx-tools/internal/format/line"
	"github.com/influxdata/influxdb/cmd/influx-tools/server"
	"go.uber.org/zap"
)

var (
	_ line.Writer
	_ binary.Writer
)

// Command represents the program execution for "store query".
type Command struct {
	// Standard input/output, overridden for testing.
	Stderr io.Writer
	Stdout io.Writer
	Logger *zap.Logger
	server server.Interface

	cpu *os.File
	mem *os.File

	configPath      string
	cpuProfile      string
	memProfile      string
	database        string
	rp              string
	shardDuration   time.Duration
	retentionPolicy string
	startTime       int64
	endTime         int64
	format          string
	print           bool
}

// NewCommand returns a new instance of Command.
func NewCommand(server server.Interface) *Command {
	return &Command{
		Stderr: os.Stderr,
		Stdout: os.Stdout,
		server: server,
	}
}

func (cmd *Command) Run(args []string) (err error) {
	err = cmd.parseFlags(args)
	if err != nil {
		return err
	}

	err = cmd.server.Open(cmd.configPath)
	if err != nil {
		return err
	}

	e, err := cmd.openExporter()
	if err != nil {
		return err
	}
	defer e.Close()

	if cmd.print {
		e.PrintPlan(os.Stdout)
		return nil
	}

	cmd.startProfile()
	defer cmd.stopProfile()

	var wr format.Writer
	switch cmd.format {
	case "line":
		wr = line.NewWriter(os.Stdout)

	case "binary":
		wr = binary.NewWriter(os.Stdout, cmd.database, cmd.rp, cmd.shardDuration)
	}
	defer func() {
		err = wr.Close()
	}()

	return e.WriteTo(wr)
}

func (cmd *Command) openExporter() (*Exporter, error) {
	cfg := &ExporterConfig{Database: cmd.database, RP: cmd.rp, ShardDuration: cmd.shardDuration}
	e, err := NewExporter(cmd.server, cfg)
	if err != nil {
		return nil, err
	}

	return e, e.Open()
}

func (cmd *Command) parseFlags(args []string) error {
	fs := flag.NewFlagSet("export", flag.ContinueOnError)
	fs.StringVar(&cmd.configPath, "config", "", "Config file")
	fs.StringVar(&cmd.cpuProfile, "cpuprofile", "", "")
	fs.StringVar(&cmd.memProfile, "memprofile", "", "")
	fs.StringVar(&cmd.database, "database", "", "Database name")
	fs.StringVar(&cmd.rp, "rp", "", "Retention policy name")
	fs.StringVar(&cmd.format, "format", "line", "Output format (line, binary)")
	fs.BoolVar(&cmd.print, "print", false, "Print plan to stdout")
	fs.DurationVar(&cmd.shardDuration, "duration", time.Hour*24*7, "Target shard duration")
	if err := fs.Parse(args); err != nil {
		return err
	}

	if cmd.database == "" {
		return errors.New("database is required")
	}

	if cmd.format != "line" && cmd.format != "binary" {
		return fmt.Errorf("invalid format '%s'", cmd.format)
	}

	return nil
}

// StartProfile initializes the cpu and memory profile, if specified.
func (cmd *Command) startProfile() {
	if cmd.cpuProfile != "" {
		f, err := os.Create(cmd.cpuProfile)
		if err != nil {
			fmt.Fprintf(cmd.Stderr, "cpuprofile: %v\n", err)
			os.Exit(1)
		}
		cmd.cpu = f
		pprof.StartCPUProfile(cmd.cpu)
	}

	if cmd.memProfile != "" {
		f, err := os.Create(cmd.memProfile)
		if err != nil {
			fmt.Fprintf(cmd.Stderr, "memprofile: %v\n", err)
			os.Exit(1)
		}
		cmd.mem = f
		runtime.MemProfileRate = 4096
	}

}

// StopProfile closes the cpu and memory profiles if they are running.
func (cmd *Command) stopProfile() {
	if cmd.cpu != nil {
		pprof.StopCPUProfile()
		cmd.cpu.Close()
	}
	if cmd.mem != nil {
		pprof.Lookup("heap").WriteTo(cmd.mem, 0)
		cmd.mem.Close()
	}
}
