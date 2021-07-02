package main

import (
	"fmt"
	"github.com/netf/dagger/pkg/deployer"
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
	app.Usage = "DAG deployment tool"

	myFlags := []cli.Flag{
		cli.StringFlag{
			Name:  "dagList",
			Value: "./config/running_dags.txt",
		},
		cli.StringFlag{
			Name:  "dagsFolder",
			Value: "./dags",
		},
		cli.StringFlag{
			Name:     "project",
			Value:    "",
			Required: true,
		},
		cli.StringFlag{
			Name:     "region",
			Value:    "",
			Required: true,
		},
		cli.StringFlag{
			Name:     "composerEnv",
			Value:    "",
			Required: true,
		},
	}
	// we create our commands
	app.Commands = []cli.Command{
		{
			Name:  "gcp",
			Usage: "GCP Composer",
			Flags: myFlags,
			Action: func(c *cli.Context) error {
				composer := deployer.ComposerEnv{
					Name:           c.String("composerEnv"),
					Project:        c.String("project"),
					Location:       c.String("region"),
					DagBucketPrefix: c.String("dagBucketPrefix"),
					LocalDagsPrefix: c.String("dagsFolder"),
				}
				err := composer.Configure()
				if err != nil {
					fmt.Errorf("configure error #{err}")
				}
				dagsToStop, dagsToStart := composer.GetStopAndStartDags(c.String("dagList"))
				composer.StopDags(dagsToStop)
				composer.StartDags(c.String("dagsFolder"), dagsToStart)
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
