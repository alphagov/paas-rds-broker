package rdsbroker

type DBExtension struct {
	Name                   string
	RequiresPreloadLibrary bool
}

// Lists the supported database extensions
// that require libraries to be loaded on startup,
// keyed on database engine family
var SupportedPreloadExtensions = map[string][]DBExtension{
	"postgres12": {
		DBExtension{
			Name:                   "auto_explain",
			RequiresPreloadLibrary: true,
		},
		DBExtension{
			Name:                   "orafce",
			RequiresPreloadLibrary: true,
		},
		DBExtension{
			Name:                   "pgaudit",
			RequiresPreloadLibrary: true,
		},
		DBExtension{
			Name:                   "pglogical",
			RequiresPreloadLibrary: true,
		},
		DBExtension{
			Name:                   "pg_similarity",
			RequiresPreloadLibrary: true,
		},
		DBExtension{
			Name:                   "pg_stat_statements",
			RequiresPreloadLibrary: true,
		},
		DBExtension{
			Name:                   "pg_hint_plan",
			RequiresPreloadLibrary: true,
		},
	},

	"postgres11": {
		DBExtension{
			Name:                   "auto_explain",
			RequiresPreloadLibrary: true,
		},
		DBExtension{
			Name:                   "orafce",
			RequiresPreloadLibrary: true,
		},
		DBExtension{
			Name:                   "pgaudit",
			RequiresPreloadLibrary: true,
		},
		DBExtension{
			Name:                   "pglogical",
			RequiresPreloadLibrary: true,
		},
		DBExtension{
			Name:                   "pg_similarity",
			RequiresPreloadLibrary: true,
		},
		DBExtension{
			Name:                   "pg_stat_statements",
			RequiresPreloadLibrary: true,
		},
		DBExtension{
			Name:                   "pg_hint_plan",
			RequiresPreloadLibrary: true,
		},
	},

	"postgres10": {
		DBExtension{
			Name:                   "auto_explain",
			RequiresPreloadLibrary: true,
		},
		DBExtension{
			Name:                   "orafce",
			RequiresPreloadLibrary: true,
		},
		DBExtension{
			Name:                   "pgaudit",
			RequiresPreloadLibrary: true,
		},
		DBExtension{
			Name:                   "pglogical",
			RequiresPreloadLibrary: true,
		},
		DBExtension{
			Name:                   "pg_similarity",
			RequiresPreloadLibrary: true,
		},
		DBExtension{
			Name:                   "pg_stat_statements",
			RequiresPreloadLibrary: true,
		},
		DBExtension{
			Name:                   "pg_hint_plan",
			RequiresPreloadLibrary: true,
		},
	},

	"mysql5.7": []DBExtension{},
}
