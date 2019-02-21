package rdsbroker

type DBExtension struct {
	Name                   string
	RequiresPreloadLibrary bool
}

// Lists the supported database extensions
// that require libraries to be loaded on startup,
// keyed on database engine family
var SupportedPreloadExtensions = map[string][]DBExtension{
	"postgres10": {
		DBExtension{
			Name:                   "pg_stat_statements",
			RequiresPreloadLibrary: true,
		},
	},

	"postgres9.5": {
		DBExtension{
			Name:                   "pg_stat_statements",
			RequiresPreloadLibrary: true,
		},
	},

	"mysql5.7": []DBExtension{},
}
