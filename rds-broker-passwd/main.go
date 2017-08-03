package main

import (
	"flag"
	"fmt"

	"github.com/alphagov/paas-rds-broker/rdsbroker"
	"github.com/alphagov/paas-rds-broker/utils"
)

var (
	seed = flag.String("seed", "", "master password seed")
	uuid = flag.String("id", "", "rds instance uuid")
)

func init() {
	flag.Parse()
}

func main() {
	if seed == nil || *seed == "" {
		fmt.Printf("master password seed: ")
		fmt.Scanln(seed)
		fmt.Printf("\n")
	}
	if uuid == nil || *uuid == "" {
		fmt.Printf("instance uuid: ")
		fmt.Scanln(uuid)
		fmt.Printf("\n")
	}

	fmt.Println(utils.GetMD5B64(*seed+*uuid, rdsbroker.MasterPasswordLength))
}
