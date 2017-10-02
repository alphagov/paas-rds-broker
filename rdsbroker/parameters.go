package rdsbroker

import (
	"errors"
)

type ProvisionParameters struct {
	BackupRetentionPeriod       int64   `json:"backup_retention_period"`
	CharacterSetName            string  `json:"character_set_name"`
	DBName                      string  `json:"dbname"`
	PreferredBackupWindow       string  `json:"preferred_backup_window"`
	PreferredMaintenanceWindow  string  `json:"preferred_maintenance_window"`
	SkipFinalSnapshot           string  `json:"skip_final_snapshot"`
	RestoreFromLatestSnapshotOf *string `json:"restore_from_latest_snapshot_of"`
}

type UpdateParameters struct {
	ApplyAtMaintenanceWindow   bool   `json:"apply_at_maintenance_window"`
	BackupRetentionPeriod      int64  `json:"backup_retention_period"`
	PreferredBackupWindow      string `json:"preferred_backup_window"`
	PreferredMaintenanceWindow string `json:"preferred_maintenance_window"`
	SkipFinalSnapshot          string `json:"skip_final_snapshot"`
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

func (pp *UpdateParameters) Validate() error {
	return Validate_SkipFinalSnapshot(pp.SkipFinalSnapshot)
}
