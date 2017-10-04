package rdsbroker

type ProvisionParameters struct {
	BackupRetentionPeriod       int64   `json:"backup_retention_period"`
	CharacterSetName            string  `json:"character_set_name"`
	DBName                      string  `json:"dbname"`
	PreferredBackupWindow       string  `json:"preferred_backup_window"`
	PreferredMaintenanceWindow  string  `json:"preferred_maintenance_window"`
	SkipFinalSnapshot           *bool   `json:"skip_final_snapshot"`
	RestoreFromLatestSnapshotOf *string `json:"restore_from_latest_snapshot_of"`
}

type UpdateParameters struct {
	ApplyAtMaintenanceWindow   bool   `json:"apply_at_maintenance_window"`
	BackupRetentionPeriod      int64  `json:"backup_retention_period"`
	PreferredBackupWindow      string `json:"preferred_backup_window"`
	PreferredMaintenanceWindow string `json:"preferred_maintenance_window"`
	SkipFinalSnapshot          *bool  `json:"skip_final_snapshot"`
}

type BindParameters struct {
	// This is currently empty, but preserved to make it easier to add
	// bind-time parameters in future.
}

func (pp *ProvisionParameters) Validate() error {
	return nil
}

func (pp *UpdateParameters) Validate() error {
	return nil
}
