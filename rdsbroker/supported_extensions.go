package rdsbroker

type DBExtension struct {
	Name                   string
	RequiresPreloadLibrary bool
}

// Lists the supported database extensions
// that require libraries to be loaded on startup,
// keyed on database engine family
//
// Note that not all of these extensions necessarily
// make sense to allow in the offering configuration
var SupportedPreloadExtensions = map[string][]DBExtension{
	"postgres13": {
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

	"postgres12": {
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
}
