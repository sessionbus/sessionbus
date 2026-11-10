// SPDX-License-Identifier: GPL-3.0-only

package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/antst/sessionbus/bus/internal/federation"
)

func main() {
	if err := runWithIO(os.Args[1:], os.Stdout, os.Stderr); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(arguments []string) error {
	return runWithIO(arguments, os.Stdout, os.Stderr)
}

func runWithIO(arguments []string, stdout, stderr io.Writer) error {
	if len(arguments) == 1 && (arguments[0] == "help" || arguments[0] == "-h" || arguments[0] == "--help") {
		writeHubHelp(stdout)
		return nil
	}
	if len(arguments) == 2 && arguments[0] == "help" && arguments[1] == "add-host" {
		writeAddHostHelp(stdout)
		return nil
	}
	if len(arguments) > 0 && arguments[0] == "add-host" {
		err := addHostWithIO(arguments[1:], stdout, stderr)
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return err
	}
	set := flag.NewFlagSet("sessionbus-hub", flag.ContinueOnError)
	set.SetOutput(stderr)
	set.Usage = func() { writeHubHelp(stderr) }
	listen, path, policyPath := "", "", ""
	set.StringVar(&listen, "listen", os.Getenv("SESSIONBUS_HUB_LISTEN"), "TCP listen address")
	set.StringVar(&path, "config", os.Getenv("SESSIONBUS_HUB_CONFIG"), "mode-0600 host secret map")
	set.StringVar(&policyPath, "policy", "", "optional mode-0600 federation policy file")
	if err := set.Parse(arguments); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return err
	}
	if set.NArg() != 0 || listen == "" || path == "" {
		return errors.New("-listen and -config are required")
	}
	policyConfigured := false
	set.Visit(func(option *flag.Flag) {
		if option.Name == "policy" {
			policyConfigured = true
		}
	})
	if policyConfigured && policyPath == "" {
		return errors.New("-policy requires a nonempty path")
	}
	secrets, err := loadSecrets(path)
	if err != nil {
		return err
	}
	configuration, err := federation.ServerTLS(secrets)
	if err != nil {
		return err
	}
	var policy *federation.Policy
	if policyPath != "" {
		policy, err = federation.LoadPolicy(policyPath)
		if err == nil {
			err = policy.ValidateHosts(secrets)
		}
		if err != nil {
			return err
		}
	}
	listener, err := net.Listen("tcp", listen)
	if err != nil {
		return err
	}
	hub := federation.StartHubWithPolicy(listener, configuration, stderr, policy)
	defer hub.Close()
	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP)
	defer signal.Stop(interrupt)
	for signal := range interrupt {
		if signal != syscall.SIGHUP {
			return nil
		}
		if policyPath == "" {
			fmt.Fprintln(stderr, "federation SIGHUP ignored: --policy is not configured")
			continue
		}
		updated, err := federation.LoadPolicy(policyPath)
		if err == nil {
			err = updated.ValidateHosts(secrets)
		}
		if err != nil {
			fmt.Fprintln(stderr, err)
			continue
		}
		if !hub.ReloadPolicy(updated) {
			return nil
		}
		fmt.Fprintf(stderr, "federation policy reloaded path=%q time=%s\n", policyPath, time.Now().UTC().Format(time.RFC3339Nano))
	}
	return nil
}

func writeHubHelp(output io.Writer) {
	fmt.Fprint(output, `Usage:
  sessionbus-hub [--listen ADDRESS] [--config FILE] [--policy FILE]
  sessionbus-hub add-host --secret-file FILE [--config FILE] HOST
  sessionbus-hub help [add-host]

Commands:
  add-host  Register a host's join secret in the offline hub map.

Server options:
  --listen ADDRESS  TCP address to listen on (or SESSIONBUS_HUB_LISTEN).
  --config FILE     Mode-0600 host secret map (or SESSIONBUS_HUB_CONFIG).
  --policy FILE     Optional mode-0600 policy. SIGHUP reloads the complete file;
                    an invalid replacement keeps the previous policy.

The server requires both values. The installer defaults to :7419 and
${XDG_CONFIG_HOME:-$HOME/.config}/sessionbus/hub.json; direct invocations use only
flags or environment.

Examples:
  sessionbus-hub --listen :7419 --config "${XDG_CONFIG_HOME:-$HOME/.config}/sessionbus/hub.json"
  sessionbus-hub add-host --secret-file /path/to/host.key workstation
`)
}

func writeAddHostHelp(output io.Writer) {
	fmt.Fprint(output, `Usage:
  sessionbus-hub add-host --secret-file FILE [--config FILE] HOST

Options:
  --secret-file FILE  Mode-0600 regular file containing the host join secret.
  --config FILE       Hub map. Defaults to SESSIONBUS_HUB_CONFIG, otherwise
                      ${XDG_CONFIG_HOME:-$HOME/.config}/sessionbus/hub.json.

The command edits the offline map only. Restart sessionbus-hub to load it.

Example:
  sessionbus-hub add-host --secret-file /path/to/host.key workstation
`)
}

func loadSecrets(path string) (map[string]string, error) {
	fd, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer fd.Close()
	info, err := fd.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm() != 0o600 {
		return nil, errors.New("federation config must be a mode-0600 regular file")
	}
	decoder := json.NewDecoder(fd)
	start, err := decoder.Token()
	if err != nil || start != json.Delim('{') {
		return nil, errors.New("federation config must be a JSON object")
	}
	secrets := map[string]string{}
	for decoder.More() {
		token, tokenErr := decoder.Token()
		host, ok := token.(string)
		_, exists := secrets[host]
		if tokenErr != nil || !ok || exists {
			return nil, errors.New("invalid or duplicate federation host")
		}
		var secret string
		if decoder.Decode(&secret) != nil {
			return nil, errors.New("federation secret must be a string")
		}
		secrets[host] = secret
	}
	if _, err = decoder.Token(); err != nil {
		return nil, err
	}
	if err = decoder.Decode(&struct{}{}); err != io.EOF {
		return nil, errors.New("trailing federation config value")
	}
	return secrets, nil
}
