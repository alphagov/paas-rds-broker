package rdsbroker

import (
	"errors"
)

type ProvisionParameters struct {
	BackupRetentionPeriod      int64
	CharacterSetName           string
	DBName                     string
	PreferredBackupWindow      string
	PreferredMaintenanceWindow string
	SkipFinalSnapshot          string `mapstructure:"skip_final_snapshot"`
}

type UpdateParameters struct {
	ApplyImmediately           bool
	BackupRetentionPeriod      int64
	PreferredBackupWindow      string
	PreferredMaintenanceWindow string
	SkipFinalSnapshot          string `mapstructure:"skip_final_snapshot"`
	ReadReplicaCount           int    `mapstructure:"read_replica_count"`
}

type BindParameters struct {
	// This is currently empty, but preserved to make it easier to add
	// bind-time parameters in future.
}

func Validate_SkipFinalSnapshot(SkipFinalSnapshot string) error {
	switch SkipFinalSnapshot {
	case "true", "false", "":
		return nil
	}
	return errors.New("skip_final_snapshot must be set to true or false, or not set at all")
}

func (pp *ProvisionParameters) Validate() error {
	return Validate_SkipFinalSnapshot(pp.SkipFinalSnapshot)
}

func (pp *UpdateParameters) Validate(rp RDSProperties) error {
	if err := Validate_SkipFinalSnapshot(pp.SkipFinalSnapshot); err != nil {
		return err
	}
	if pp.ReadReplicaCount > 0 && !rp.AllowReadReplicas {
		return errors.New("plan does not allow read replicas")
	}
	return nil
}
