package sqlengine

import (
	"fmt"
	"strings"
	"unicode"

	"github.com/lib/pq" // PostgreSQL Driver
)

type PrivAction int

const (
	Revoke PrivAction = iota
	Grant
)

func (pa PrivAction) String() string {
	return [...]string{"REVOKE", "GRANT"}[pa]
}

func (pa PrivAction) Preposition() string {
	return [...]string{"FROM", "TO"}[pa]
}

type PostgresqlPrivilege struct {
	TargetType   string    `json:"target_type"`
	TargetSchema *string   `json:"target_schema"`
	TargetName   *string   `json:"target_name"`
	Privilege    string    `json:"privilege"`
	ColumnNames  *[]string `json:"column_names"`
}

func (pp *PostgresqlPrivilege) Validate() error {
	switch strings.ToUpper(pp.TargetType) {
		case "TABLE":
			if pp.TargetName == nil || *pp.TargetName == "" {
				return fmt.Errorf("Must provide a non-empty target_name for 'TABLE' postgresql privilege target_type (%+v)", *pp)
			}

			if err := ValidatePostgresqlName(*pp.TargetName); err != nil {
				return err
			}

			if pp.TargetSchema != nil && *pp.TargetSchema != "" {
				if err := ValidatePostgresqlName(*pp.TargetSchema); err != nil {
					return err
				}
			}

			if pp.ColumnNames != nil && len(*pp.ColumnNames) != 0 {
				for _, columnName := range *pp.ColumnNames {
					if err := ValidatePostgresqlName(columnName); err != nil {
						return err
					}
				}

				switch strings.ToUpper(pp.Privilege) {
					case "SELECT":
					case "INSERT":
					case "UPDATE":
					case "REFERENCES":
					case "ALL":
					default:
						return fmt.Errorf("Unknown postgresql column privilege: %s", pp.Privilege)
				}
			} else {
				switch strings.ToUpper(pp.Privilege) {
					case "SELECT":
					case "INSERT":
					case "UPDATE":
					case "DELETE":
					case "TRUNCATE":
					case "REFERENCES":
					case "TRIGGER":
					case "ALL":
					default:
						return fmt.Errorf("Unknown postgresql table privilege: %s", pp.Privilege)
				}
			}
		case "SEQUENCE":
			if pp.TargetName == nil || *pp.TargetName == "" {
				return fmt.Errorf("Must provide a non-empty target_name for 'SEQUENCE' postgresql privilege target_type (%+v)", *pp)
			}

			if err := ValidatePostgresqlName(*pp.TargetName); err != nil {
				return err
			}

			if pp.TargetSchema != nil && *pp.TargetSchema != "" {
				if err := ValidatePostgresqlName(*pp.TargetSchema); err != nil {
					return err
				}
			}

			if pp.ColumnNames != nil {
				return fmt.Errorf("column_names makes no sense for 'SEQUENCE' postgresql privilege target_type (%+v)", *pp)
			}

			switch strings.ToUpper(pp.Privilege) {
				case "USAGE":
				case "SELECT":
				case "UPDATE":
				case "ALL":
				default:
					return fmt.Errorf("Unknown postgresql sequence privilege: %s", pp.Privilege)
			}
		case "DATABASE":
			if pp.TargetName != nil {
				return fmt.Errorf("target_name makes no sense for 'DATABASE' postgresql privilege target_type (%+v)", *pp)
			}

			if pp.TargetSchema != nil {
				return fmt.Errorf("target_schema makes no sense for 'DATABASE' postgresql privilege target_type (%+v)", *pp)
			}

			if pp.ColumnNames != nil {
				return fmt.Errorf("column_names makes no sense for 'DATABASE' postgresql privilege target_type (%+v)", *pp)
			}

			switch strings.ToUpper(pp.Privilege) {
				case "TEMPORARY":
				case "TEMP":
				case "ALL":
				default:
					return fmt.Errorf("Unknown postgresql database privilege: %s", pp.Privilege)
			}
		case "SCHEMA":
			if pp.TargetName == nil || *pp.TargetName == "" {
				return fmt.Errorf("Must provide a non-empty target_name for 'SCHEMA' postgresql privilege target_type (%+v)", *pp)
			}

			if err := ValidatePostgresqlName(*pp.TargetName); err != nil {
				return err
			}

			if pp.TargetSchema != nil {
				return fmt.Errorf("target_schema makes no sense for 'SCHEMA' postgresql privilege target_type (try target_name instead) (%+v)", *pp)
			}

			if pp.ColumnNames != nil {
				return fmt.Errorf("column_names makes no sense for 'SCHEMA' postgresql privilege target_type (%+v)", *pp)
			}

			switch strings.ToUpper(pp.Privilege) {
				case "USAGE":
				case "ALL":
				default:
					return fmt.Errorf("Unknown postgresql schema privilege: %s", pp.Privilege)
			}
		default:
			return fmt.Errorf("Unknown postgresql privilege target_type: %s", pp.TargetType)
	}

	return nil
}

func getQuotedIdents(schema *string, name string) string {
	if schema == nil || *schema == "" {
		return pq.QuoteIdentifier(name)
	}

	return fmt.Sprintf("%s.%s", pq.QuoteIdentifier(*schema), pq.QuoteIdentifier(name))
}

const privilegeStatementWrapper = `BEGIN
			%s
		EXCEPTION
			WHEN undefined_column OR undefined_table OR invalid_schema_name THEN
				NULL;
		END;`

func (pp *PostgresqlPrivilege) getPlPgSQL(action PrivAction) string {
	uTargetType := strings.ToUpper(pp.TargetType)
	uPrivilege := strings.ToUpper(pp.Privilege)

	if uTargetType == "TABLE" && pp.ColumnNames != nil && len(*pp.ColumnNames) != 0 {
		quoteIdentedCols := make([]string, len(*pp.ColumnNames))
		for i, col := range *pp.ColumnNames {
			quoteIdentedCols[i] = pq.QuoteIdentifier(col)
		}

		return fmt.Sprintf(
			privilegeStatementWrapper,
			fmt.Sprintf(
				"EXECUTE '%s %s (' || %s || ') ON %s ' || %s || ' %s ' || username;",
				action,
				uPrivilege,
				pq.QuoteLiteral(strings.Join(quoteIdentedCols, ", ")),
				uTargetType,
				pq.QuoteLiteral(getQuotedIdents(pp.TargetSchema, *pp.TargetName)),
				action.Preposition(),
			),
		)
	}
	if uTargetType == "DATABASE" {
		return fmt.Sprintf(
			privilegeStatementWrapper,
			fmt.Sprintf(
				"EXECUTE '%s %s ON %s ' || dbname || ' %s ' || username;",
				action,
				uPrivilege,
				uTargetType,
				action.Preposition(),
			),
		)
	}
	if uTargetType == "SCHEMA" {
		return fmt.Sprintf(
			privilegeStatementWrapper,
			fmt.Sprintf(
				"EXECUTE '%s %s ON %s ' || %s || ' %s ' || username;",
				action,
				uPrivilege,
				uTargetType,
				pq.QuoteLiteral(pq.QuoteIdentifier(*pp.TargetName)),
				action.Preposition(),
			),
		)
	}

	return fmt.Sprintf(
		privilegeStatementWrapper,
		fmt.Sprintf(
			"EXECUTE '%s %s ON %s ' || %s || ' %s ' || username;",
			action,
			uPrivilege,
			uTargetType,
			pq.QuoteLiteral(getQuotedIdents(pp.TargetSchema, *pp.TargetName)),
			action.Preposition(),
		),
	)
}

func ValidatePostgresqlName(name string) error {
	if len(name) == 0 {
		return fmt.Errorf("Empty name")
	}

	for i := 0; i < len(name); i++ {
		if name[i] > unicode.MaxASCII {
			return fmt.Errorf("Non-ASCII characters in postgresql object names not (yet) supported: %s", name)
		}
	}

	return nil
}

type PostgresUserBindParameters struct {
	IsOwner                *bool                  `json:"is_owner"`
	DefaultPrivilegePolicy string                 `json:"default_privilege_policy"`
	RevokePrivileges       *[]PostgresqlPrivilege `json:"revoke_privileges"`
	GrantPrivileges        *[]PostgresqlPrivilege `json:"grant_privileges"`
}

func (bp *PostgresUserBindParameters) Validate(pgServerVersionNum int) error {
	if bp.IsOwner != nil && !*bp.IsOwner {
		switch strings.ToLower(bp.DefaultPrivilegePolicy) {
			case "revoke":
				if bp.RevokePrivileges != nil {
					return fmt.Errorf("revoke_privileges makes no sense with default_privilege_policy 'revoke' (%+v)", *bp)
				}
				if bp.GrantPrivileges != nil {
					for _, privilege := range *bp.GrantPrivileges {
						if err := privilege.Validate(); err != nil {
							return err
						}
					}
				}
			case "grant":
				if pgServerVersionNum < 100000 {
					return fmt.Errorf("default_privilege_policy 'grant' not supported for PostgreSQL versions <10")
				}
				if bp.GrantPrivileges != nil {
					return fmt.Errorf("grant_privileges makes no sense with default_privilege_policy 'grant' (%+v)", *bp)
				}
				if bp.RevokePrivileges != nil {
					for _, privilege := range *bp.RevokePrivileges {
						if err := privilege.Validate(); err != nil {
							return err
						}
					}
				}
			default:
				return fmt.Errorf("default_privilege_policy must be one of 'grant' or 'revoke' (%+v)", *bp)
		}
	} else {
		if bp.DefaultPrivilegePolicy != "" {
			return fmt.Errorf("postgresql_user.default_privilege_policy makes no sense for owner (%+v)", *bp)
		}
		if bp.RevokePrivileges != nil {
			return fmt.Errorf("postgresql_user.revoke_privileges makes no sense for owner (%+v)", *bp)
		}
		if bp.GrantPrivileges != nil {
			return fmt.Errorf("postgresql_user.grant_privileges makes no sense for owner (%+v)", *bp)
		}
	}
	return nil
}

const grantAllPrivilegesFragment = `FOR r IN SELECT schema_name FROM information_schema.schemata WHERE schema_name != 'information_schema' AND schema_name NOT LIKE 'pg_%' LOOP
			EXECUTE 'GRANT ALL ON ALL TABLES IN SCHEMA ' || quote_ident(r.schema_name) || ' TO ' || username;
			EXECUTE 'GRANT ALL ON ALL SEQUENCES IN SCHEMA ' || quote_ident(r.schema_name) || ' TO ' || username;
			EXECUTE 'GRANT ALL ON SCHEMA ' || quote_ident(r.schema_name) || ' TO ' || username;
		END LOOP;

		EXECUTE 'GRANT ALL ON DATABASE ' || dbname || ' TO ' || username;

		EXECUTE 'ALTER DEFAULT PRIVILEGES GRANT ALL ON TABLES TO ' || username;
		EXECUTE 'ALTER DEFAULT PRIVILEGES GRANT ALL ON SEQUENCES TO ' || username;
		EXECUTE 'ALTER DEFAULT PRIVILEGES GRANT ALL ON SCHEMAS TO ' || username;`

const commonDefaultPrivilegeFragment = `EXECUTE 'GRANT CONNECT ON DATABASE ' || dbname || ' TO ' || username;`

const immediatePlPgSQLWrapper = `
	DECLARE
		username text := %s;
		dbname text := %s;
		r RECORD;
	BEGIN
		%s
	END`

func (bp *PostgresUserBindParameters) GetDefaultPrivilegePlPgSQL(username string, dbname string) string {
	var statementBuilder strings.Builder

	if strings.ToLower(bp.DefaultPrivilegePolicy) != "revoke" {
		// grant priviliges to all objects which can then be revoked individually
		statementBuilder.WriteString(grantAllPrivilegesFragment)
		statementBuilder.WriteString("\n\n\t\t")
	}

	statementBuilder.WriteString(commonDefaultPrivilegeFragment)

	return fmt.Sprintf(
		immediatePlPgSQLWrapper,
		pq.QuoteLiteral(pq.QuoteIdentifier(username)),
		pq.QuoteLiteral(pq.QuoteIdentifier(dbname)),
		statementBuilder.String(),
	)
}

func (bp *PostgresUserBindParameters) GetPrivilegeAssignmentPlPgSQL(username string, dbname string) string {
	var privs *[]PostgresqlPrivilege
	var privsAction PrivAction
	if strings.ToLower(bp.DefaultPrivilegePolicy) != "revoke" {
		privs = bp.RevokePrivileges
		privsAction = Revoke
	} else {
		privs = bp.GrantPrivileges
		privsAction = Grant
	}

	if privs == nil || len(*privs) == 0 || bp.IsOwner == nil || *bp.IsOwner {
		return ""
	}

	var privsBuilder strings.Builder
	for i, priv := range *privs {
		privPlPgSQL := priv.getPlPgSQL(privsAction)

		if i != 0 {
			privsBuilder.WriteString("\n\t\t")
		}
		privsBuilder.WriteString(privPlPgSQL)
	}

	return fmt.Sprintf(
		immediatePlPgSQLWrapper,
		pq.QuoteLiteral(pq.QuoteIdentifier(username)),
		pq.QuoteLiteral(pq.QuoteIdentifier(dbname)),
		privsBuilder.String(),
	)
}
