package rdsbroker

import "fmt"

type ProvisionParameters struct {
	BackupRetentionPeriod           int64    `json:"backup_retention_period"`
	CharacterSetName                string   `json:"character_set_name"`
	DBName                          string   `json:"dbname"`
	PreferredBackupWindow           string   `json:"preferred_backup_window"`
	PreferredMaintenanceWindow      string   `json:"preferred_maintenance_window"`
	SkipFinalSnapshot               *bool    `json:"skip_final_snapshot"`
	RestoreFromLatestSnapshotOf     *string  `json:"restore_from_latest_snapshot_of"`
	RestoreFromLatestSnapshotBefore *string  `json:"restore_from_latest_snapshot_before"`
	Extensions                      []string `json:"enable_extensions"`
}

type UpdateParameters struct {
	ApplyAtMaintenanceWindow   bool     `json:"apply_at_maintenance_window"`
	BackupRetentionPeriod      int64    `json:"backup_retention_period"`
	PreferredBackupWindow      string   `json:"preferred_backup_window"`
	PreferredMaintenanceWindow string   `json:"preferred_maintenance_window"`
	SkipFinalSnapshot          *bool    `json:"skip_final_snapshot"`
	Reboot                     *bool    `json:"reboot"`
	ForceFailover              *bool    `json:"force_failover"`
	EnableExtensions           []string `json:"enable_extensions"`
	DisableExtensions          []string `json:"disable_extensions"`
}

type BindParameters struct {
	GrantReplication	bool	`json:"grant_replication"`
}

func (pp *ProvisionParameters) Validate() error {
	return nil
}

func (up *UpdateParameters) Validate() error {
	for _, ext1 := range up.EnableExtensions {
		for _, ext2 := range up.DisableExtensions {
			if ext1 == ext2 {
				return fmt.Errorf("%s is set in both enable_extensions and disable_extensions", ext1)
			}
		}
	}
	return nil
}

func (up *UpdateParameters) CheckForCompatibilityWithPlanChange() error {
	if up.Reboot != nil && *up.Reboot {
		return fmt.Errorf("Invalid to reboot and update plan in the same command")
	}
	if len(up.EnableExtensions) > 0 {
		return fmt.Errorf("Invalid to enable extensions and update plan in the same command")
	}
	if len(up.DisableExtensions) > 0 {
		return fmt.Errorf("Invalid to disable extensions and update plan in the same command")
	}
	return nil
}

func (bp *BindParameters) Validate() error {
	return nil
}
