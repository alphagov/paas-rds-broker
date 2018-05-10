package main

import (
	"fmt"

	"github.com/alphagov/paas-rds-broker/rdsbroker"
	"github.com/alphagov/paas-rds-broker/utils"
)

func main() {
	var seed, instance string

	fmt.Printf("master password seed: ")
	fmt.Scanln(&seed)
	fmt.Printf("instance uuid: ")
	fmt.Scanln(&instance)

	fmt.Printf("\n%s\n", utils.GenerateHash(seed+instance, rdsbroker.MasterPasswordLength))
}
