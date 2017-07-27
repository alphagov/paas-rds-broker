package main

import (
	"fmt"

	"github.com/alphagov/paas-rds-broker/rdsbroker"
	"github.com/alphagov/paas-rds-broker/utils"
)

func main() {
	var seed, instance string

	fmt.Printf("master_password_seed: ")
	fmt.Scanln(&seed)
	fmt.Printf("instance_id: ")
	fmt.Scanln(&instance)

	fmt.Printf("\n%s\n", utils.GetMD5B64(seed+instance, rdsbroker.MasterPasswordLength))
}
