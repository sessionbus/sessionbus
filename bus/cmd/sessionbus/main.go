// SPDX-License-Identifier: GPL-3.0-only

package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"runtime/debug"
	"strings"
	"syscall"

	"github.com/antst/sessionbus/bus/internal/commslog"
	"github.com/antst/sessionbus/bus/internal/daemon"
	"github.com/antst/sessionbus/bus/internal/federation"
	sessionkit "github.com/antst/sessionbus/bus/sdk/go"
)

// Release archives set these values at build time. Ordinary go builds retain
// Go's VCS metadata and identify themselves as development builds.
var buildVersion = "development"
var buildRevision string

func versionString() string {
	revision := buildRevision
	if revision == "" {
		if info, ok := debug.ReadBuildInfo(); ok {
			for _, setting := range info.Settings {
				if setting.Key == "vcs.revision" {
					revision = setting.Value
				}
			}
			for _, setting := range info.Settings {
				if setting.Key == "vcs.modified" && setting.Value == "true" && revision != "" {
					revision += "-dirty"
				}
			}
		}
	}
	if revision == "" {
		revision = "unknown"
	}
	return fmt.Sprintf("sessionbus %s (%s)", buildVersion, revision)
}

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(arguments []string) error {
	return runTo(arguments, os.Stdout)
}

func runTo(arguments []string, stdout io.Writer) error {
	if len(arguments) == 1 && (arguments[0] == "-version" || arguments[0] == "--version" || arguments[0] == "-v") {
		_, err := fmt.Fprintln(stdout, versionString())
		return err
	}
	if len(arguments) > 0 && arguments[0] == "help" {
		switch {
		case len(arguments) == 1:
			arguments = []string{"--help"}
		case len(arguments) == 2 && arguments[1] == "roster":
			arguments = []string{"roster", "--help"}
		case len(arguments) == 2 && arguments[1] == "secret":
			arguments = []string{"secret", "--help"}
		case len(arguments) == 3 && arguments[1] == "lane" && arguments[2] == "archive":
			arguments = []string{"lane", "archive", "--help"}
		default:
			return errors.New("usage: sessionbus help [roster|secret|lane archive]")
		}
	}
	if len(arguments) == 2 && arguments[0] == "secret" && (arguments[1] == "--help" || arguments[1] == "-h") {
		_, err := fmt.Fprintln(stdout, "Usage: sessionbus secret\n\nGenerate a fresh base64 federation join secret on stdout. This command does not\ninstall or rotate any key. Keep the output private; host installation normally\ngenerates and preserves ${XDG_CONFIG_HOME:-$HOME/.config}/sessionbus/host.key.")
		return err
	}
	if len(arguments) > 0 && arguments[0] == "roster" {
		return runRoster(arguments[1:], stdout)
	}
	if len(arguments) > 0 && arguments[0] == "lane" {
		return runLane(arguments[1:], stdout)
	}
	if len(arguments) == 1 && arguments[0] == "secret" {
		secret, err := federation.NewSecret()
		if err == nil {
			_, err = fmt.Fprintln(stdout, secret)
		}
		return err
	}
	configuration, err := parseTo(arguments, stdout)
	if err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return err
	}
	service, err := daemon.Start(configuration)
	if err != nil {
		return err
	}
	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(interrupt)
	select {
	case <-interrupt:
	case <-service.Done():
	}
	return service.Close()
}

func parse(arguments []string) (daemon.Config, error) { return parseTo(arguments, os.Stderr) }

func parseTo(arguments []string, output io.Writer) (daemon.Config, error) {
	root, err := stateRoot()
	if err != nil {
		return daemon.Config{}, err
	}
	socket := sessionkit.Socket()
	set := flag.NewFlagSet("sessionbus", flag.ContinueOnError)
	set.SetOutput(output)
	set.Usage = func() { fmt.Fprint(output, daemonHelp); set.PrintDefaults() }
	configuration := daemon.Config{}
	products := ""
	logMode := os.Getenv("SESSIONBUS_COMMS_LOG")
	if logMode == "" {
		logMode = "off"
	}
	logDir := os.Getenv("SESSIONBUS_COMMS_LOG_DIR")
	if logDir == "" {
		logDir = filepath.Join(root, "comms")
	}
	var logSessions, logGroups string
	set.StringVar(&logMode, "comms-log", logMode, "communication log: off, metadata, or content (SESSIONBUS_COMMS_LOG)")
	set.StringVar(&logDir, "comms-log-dir", logDir, "private rotated JSONL directory (SESSIONBUS_COMMS_LOG_DIR)")
	set.Int64Var(&configuration.CommsLog.MaxFileBytes, "comms-log-max-bytes", 16<<20, "maximum bytes per communication log file")
	set.IntVar(&configuration.CommsLog.MaxFiles, "comms-log-files", 4, "retained communication log files, including current")
	set.IntVar(&configuration.CommsLog.QueueBytes, "comms-log-queue-bytes", 4<<20, "maximum queued communication log bytes")
	set.StringVar(&logSessions, "comms-log-sessions", "", "optional comma-separated canonical session IDs to include")
	set.StringVar(&logGroups, "comms-log-groups", "", "optional comma-separated groups to include")
	set.StringVar(&configuration.SocketPath, "socket", socket, "unix socket path")
	set.StringVar(&configuration.TablePath, "table", filepath.Join(root, "sessions.json"), "durable session table")
	set.StringVar(&configuration.Host, "host", os.Getenv("SESSIONBUS_HOST"), "local host name")
	set.StringVar(&products, "products", os.Getenv("SESSIONBUS_PRODUCTS"), "comma-separated advertised products (default SESSIONBUS_PRODUCTS)")
	set.StringVar(&configuration.HubAddress, "hub", os.Getenv("SESSIONBUS_HUB"), "federation hub address")
	set.StringVar(&configuration.HubSecret, "hub-secret", "", "federation host secret")
	var secretFile string
	set.StringVar(&secretFile, "hub-secret-file", os.Getenv("SESSIONBUS_HUB_SECRET_FILE"), "mode-0600 federation secret file (used when connecting to a hub)")
	if err := set.Parse(arguments); err != nil {
		return daemon.Config{}, err
	}
	if set.NArg() != 0 {
		return daemon.Config{}, fmt.Errorf("unexpected argument %q", set.Arg(0))
	}
	if secretFile != "" && configuration.HubAddress != "" {
		if configuration.HubSecret != "" {
			return daemon.Config{}, fmt.Errorf("use only one of -hub-secret and -hub-secret-file")
		}
		info, err := os.Stat(secretFile)
		if err != nil || !info.Mode().IsRegular() || info.Mode().Perm() != 0600 {
			return daemon.Config{}, fmt.Errorf("hub secret file must be a mode-0600 regular file")
		}
		b, err := os.ReadFile(secretFile)
		if err != nil {
			return daemon.Config{}, err
		}
		configuration.HubSecret = strings.TrimSpace(string(b))
	}
	switch commslog.Mode(logMode) {
	case commslog.Off, commslog.Metadata, commslog.Content:
	default:
		return daemon.Config{}, fmt.Errorf("invalid comms-log mode %q", logMode)
	}
	configuration.CommsLog.Mode = commslog.Mode(logMode)
	configuration.CommsLog.Path = filepath.Join(logDir, "sessionbus.jsonl")
	if logSessions != "" {
		configuration.CommsLog.Sessions = strings.Split(logSessions, ",")
	}
	if logGroups != "" {
		configuration.CommsLog.Groups = strings.Split(logGroups, ",")
	}
	configuration.CommsLog.OnError = func(err error) { fmt.Fprintf(os.Stderr, "Sessionbus communication log stopped: %v\n", err) }
	if products != "" {
		configuration.Products = strings.Split(products, ",")
	}
	return configuration, nil
}

func stateRoot() (string, error) {
	if root := os.Getenv("XDG_STATE_HOME"); root != "" {
		return filepath.Abs(filepath.Join(root, "sessionbus"))
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return "", err
	}
	return filepath.Abs(filepath.Join(home, ".local", "state", "sessionbus"))
}

const daemonHelp = `Sessionbus — local and federated session router

Usage:
  sessionbus [daemon flags]           Run the daemon in the foreground
  sessionbus roster [--all] [--json] [--local]  Inspect online peers and active lanes
  sessionbus lane archive [flags] <id>  Stop a local lane; retain history/resume
  sessionbus secret                  Generate a private federation join secret
  sessionbus -version                Show the release and source revision
  sessionbus help [roster|secret|lane archive]  Show command help

Examples:
  sessionbus roster
  sessionbus roster --json
  sessionbus roster --all            Include offline and archived entries
  sessionbus roster --local --socket /path/to/presence.sock

The binary has no implicit install, start, or restart subcommand. Installed
services are managed by systemd --user (Linux) or launchd (macOS):
  systemctl --user status sessionbus
  systemctl --user restart sessionbus
  launchctl kickstart -k gui/$(id -u)/net.antst.sessionbus

Socket discovery: SESSIONBUS_SOCKET, then $XDG_RUNTIME_DIR/sessionbus/presence.sock,
or /tmp/sessionbus-<uid>/presence.sock. State: $XDG_STATE_HOME/sessionbus,
or ~/.local/state/sessionbus. Federation flags default from SESSIONBUS_HOST,
SESSIONBUS_HUB, and SESSIONBUS_HUB_SECRET_FILE. The installer preserves those
settings in ${XDG_CONFIG_HOME:-$HOME/.config}/sessionbus/service.env; the service
manager loads that file. SESSIONBUS_PRODUCTS supplies the comma-separated
advertised product list; -products overrides it (an empty value clears it).
This advertisement does not restrict lane launches or install product binaries.
Prefer -hub-secret-file over placing a secret in process arguments.
Hub outages do not stop local service: the daemon retries its connection with
bounded backoff. Lost remote operations fail without automatic replay.

Communication logging defaults to off. Set SESSIONBUS_COMMS_LOG=metadata or
content in service.env and restart the daemon. One rotated JSONL stream per host
records explicit from/to identities and correlated message/delivery IDs. Content
mode also retains message text; native prompts and Run result bodies are omitted. Default retention is
four files of 16 MiB each under the state directory/comms. The optional session
and group filters select events; they do not create duplicate per-group files.
Logging is diagnostic: queue overflow is recorded as gaps; disk errors stop the
logger and are reported on stderr. A crash can lose the unwritten tail.

Daemon flags:
`
