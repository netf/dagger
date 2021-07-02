package main

import (
	"fmt"
	"github.com/netf/dagger/pkg/deploy"
	"github.com/urfave/cli"
	"log"
	"os"
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

	myFlags := []cli.Flag{
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
	}
	// we create our commands
	app.Commands = []cli.Command{
		{
			Name:  "sync",
			Usage: "Sync DAGs to GCP Composer",
			Flags: myFlags,
			Action: func(c *cli.Context) error {
				composer := deploy.ComposerEnv{
					Name:           c.String("name"),
					Project:        c.String("project"),
					Location:       c.String("location"),
					LocalDagsPrefix: c.String("dags"),
				}
				err := composer.Configure()
				if err != nil {
					fmt.Errorf("configure error #{err}")
				}
				dagsToStop, dagsToStart := composer.GetStopAndStartDags(c.String("list"))
				composer.StopDags(dagsToStop)
				composer.StartDags(c.String("dags"), dagsToStart)
				composer.StartMonitoringDag()
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
