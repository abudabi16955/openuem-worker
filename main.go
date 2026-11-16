package main

import (
	"log"
	"os"

	"github.com/open-uem/openuem-worker/internal/commands"
	"github.com/open-uem/openuem-worker/internal/crypto"
	"github.com/urfave/cli/v2"
)

func main() {
	// Fail-fast validation of OPENUEM_VAULT_KEY when starting agents worker
	if isAgentWorkerCommand(os.Args) {
		_ = crypto.MustGetVaultKey()
	}

	app := &cli.App{
		Name:      "openuem-worker",
		Commands:  getCommands(),
		Usage:     "Manage an OpenUEM worker",
		Authors:   []*cli.Author{{Name: "Miguel Angel Alvarez Cabrerizo", Email: "mcabrerizo@openuem.eu"}},
		Copyright: "2025 - Miguel Angel Alvarez Cabrerizo <https://github.com/open-uem>",
	}

	if err := app.Run(os.Args); err != nil {
		log.Fatal(err)
	}
}

func isAgentWorkerCommand(args []string) bool {
	if len(args) < 2 {
		return false
	}
	return args[1] == "agents"
}

func getCommands() []*cli.Command {
	return []*cli.Command{
		commands.AgentWorker(),
		commands.CertManagerWorker(),
		commands.NotificationsWorker(),
		commands.HealthCheck(),
	}
}
