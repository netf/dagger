package main

import (
	"fmt"
	"github.com/netf/dagger/pkg/deploy"
	"github.com/urfave/cli"
	"log"
	"os"
	"time"
)

func main() {

	BANNER := `
██████╗░░█████╗░░██████╗░░██████╗░███████╗██████╗░
██╔══██╗██╔══██╗██╔════╝░██╔════╝░██╔════╝██╔══██╗
██║░░██║███████║██║░░██╗░██║░░██╗░█████╗░░██████╔╝
██║░░██║██╔══██║██║░░╚██╗██║░░╚██╗██╔══╝░░██╔══██╗
██████╔╝██║░░██║╚██████╔╝╚██████╔╝███████╗██║░░██║
╚═════╝░╚═╝░░╚═╝░╚═════╝░░╚═════╝░╚══════╝╚═╝░░╚═╝
`
	fmt.Println(BANNER)

	app := cli.NewApp()
	app.Name = "dagger"
	app.Usage = "DAG management tool"

	flags := []cli.Flag{
		cli.StringFlag{
			Name:  "list",
			Value: "./config/running_dags.txt",
			Required: true,
			Usage: "File with DAGs to run",
		},
		cli.StringFlag{
			Name:  "dags",
			Value: "./dags",
			Usage: "DAGs folder",
			Required: true,
		},
		cli.StringFlag{
			Name:  "plugins",
			Value: "./plugins",
			Usage: "Airflow plugins",
			Required: false,
		},
		cli.StringFlag{
			Name:  "data",
			Value: "./data",
			Usage: "Airflow data (ie: sql files)",
			Required: false,
		},
		cli.StringFlag{
			Name:     "project",
			Value:    "",
			Required: true,
			Usage: "GCP project name",
		},
		cli.StringFlag{
			Name:     "location",
			Value:    "",
			Required: true,
			Usage: "GCP Composer location",
		},
		cli.StringFlag{
			Name:     "name",
			Value:    "",
			Required: true,
			Usage: "Name of GCP Composer environment",
		},
		cli.StringFlag{
			Name:     "variables",
			Value:    "",
			Required: false,
			Usage: "Airflow variables config file",
		},
		cli.StringFlag{
			Name:     "connections",
			Value:    "",
			Required: false,
			Usage: "Airflow connections config file",
		},
		cli.BoolFlag{
			Name:     "loop",
			Usage: "Run Dagger in a loop (useful for continues sync)",
		},
	}
	// we create our commands
	app.Commands = []cli.Command{
		{
			Name:  "sync",
			Usage: "Sync DAGs to GCP Composer",
			Flags: flags,
			Action: func(c *cli.Context) error {
				fmt.Printf("Composer environment: %s\n", c.String("name"))
				fmt.Printf("Project: %s, Location: %s\n", c.String("project"), c.String("location"))
				fmt.Println()
				composer := deploy.ComposerEnv{
					Name:            c.String("name"),
					Project:         c.String("project"),
					Location:        c.String("location"),
					LocalDagsDir: 	 c.String("dags"),
					LocalPluginsDir: c.String("plugins"),
					LocalDataDir: 	 c.String("data"),
					VariablesFile: 	 c.String("variables"),
					ConnectionsFile: c.String("connections"),
				}
				err := composer.Configure()
				if err != nil {
					log.Fatalf("configure error: %s", err)
				}
				err = composer.SyncPlugins()
				if err != nil {
					log.Fatalf("sync plugins error: %s", err)
				}
				err = composer.SyncData()
				if err != nil {
					log.Fatalf("sync data error: %s", err)
				}
				err = composer.ImportVariables()
				if err != nil {
					log.Fatalf("import variables error: %s", err)
				}
				err = composer.ImportConnections()
				if err != nil {
					log.Fatalf("import variables error: %s", err)
				}
				for {
					dagsToStop, dagsToStart := composer.GetStopAndStartDags(c.String("list"))
					composer.StopDags(dagsToStop)
					composer.StartDags(c.String("dags"), dagsToStart)
					composer.StartMonitoringDag()
					if !c.Bool("loop") {
						break
					}
					time.Sleep(60 * time.Second)
				}
				return nil
			},
		},
	}
	// start our application
	err := app.Run(os.Args)
	if err != nil {
		log.Fatal(err)
	}
}
