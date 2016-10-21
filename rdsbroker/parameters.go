package rdsbroker

type ProvisionParameters struct {
	BackupRetentionPeriod      int64
	CharacterSetName           string
	DBName                     string
	PreferredBackupWindow      string
	PreferredMaintenanceWindow string
	SkipFinalSnapshot          bool `mapstructure:"skip_final_snapshot"`
}

type UpdateParameters struct {
	ApplyImmediately           bool `mapstructure:"apply_immediately"`
	BackupRetentionPeriod      int64
	PreferredBackupWindow      string
	PreferredMaintenanceWindow string
	SkipFinalSnapshot          bool `mapstructure:"skip_final_snapshot"`
}

type BindParameters struct {
	// This is currently empty, but preserved to make it easier to add
	// bind-time parameters in future.
}
