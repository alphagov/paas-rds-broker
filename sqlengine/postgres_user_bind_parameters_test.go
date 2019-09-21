package sqlengine

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("PostgresUserBindParameters", func() {
	var _ = Describe("Validation", func() {
		It("returns an error for extra options used with a non-owner postgresql user", func() {
			bp := PostgresUserBindParameters {
				IsOwner: boolPointer(true),
				GrantPrivileges: &[]PostgresqlPrivilege{},
			}

			err := bp.Validate(10000)
			Expect(err).To(MatchError(ContainSubstring(`postgresql_user.grant_privileges makes no sense for owner`)))

			bp = PostgresUserBindParameters {
				RevokePrivileges: &[]PostgresqlPrivilege{},
			}

			err = bp.Validate(90501)
			Expect(err).To(MatchError(ContainSubstring(`postgresql_user.revoke_privileges makes no sense for owner`)))

			bp = PostgresUserBindParameters {
				IsOwner: boolPointer(true),
				DefaultPrivilegePolicy: "grant",
			}

			err = bp.Validate(100000)
			Expect(err).To(MatchError(ContainSubstring(`postgresql_user.default_privilege_policy makes no sense for owner`)))
		})

		It("returns an error for unexpected default_privilege_policy", func() {
			bp := PostgresUserBindParameters {
				IsOwner: boolPointer(false),
				DefaultPrivilegePolicy: "perhaps",
			}

			err := bp.Validate(90501)
			Expect(err).To(MatchError(ContainSubstring(`default_privilege_policy must be one of 'grant' or 'revoke'`)))
		})

		It("returns an error for a privileges list clashing with default_privilege_policy", func() {
			bp := PostgresUserBindParameters {
				IsOwner: boolPointer(false),
				DefaultPrivilegePolicy: "grant",
				GrantPrivileges: &[]PostgresqlPrivilege{},
			}

			err := bp.Validate(100000)
			Expect(err).To(MatchError(ContainSubstring(`grant_privileges makes no sense with default_privilege_policy 'grant'`)))

			bp = PostgresUserBindParameters {
				IsOwner: boolPointer(false),
				DefaultPrivilegePolicy: "revoke",
				RevokePrivileges: &[]PostgresqlPrivilege{},
			}

			err = bp.Validate(90501)
			Expect(err).To(MatchError(ContainSubstring(`revoke_privileges makes no sense with default_privilege_policy 'revoke'`)))
		})

		It("returns an error for for a grant default_privilege_policy on postgres 9.5", func() {
			bp := PostgresUserBindParameters {
				IsOwner: boolPointer(false),
				DefaultPrivilegePolicy: "grant",
				RevokePrivileges: &[]PostgresqlPrivilege{},
			}

			err := bp.Validate(90501)
			Expect(err).To(MatchError(ContainSubstring(`PostgreSQL version`)))
		})

		It("returns an error for unknown privilege target_type", func() {
			bp := PostgresUserBindParameters {
				IsOwner: boolPointer(false),
				DefaultPrivilegePolicy: "grant",
				RevokePrivileges: &[]PostgresqlPrivilege{
					PostgresqlPrivilege{
						TargetType: "foo",
					},
				},
			}

			err := bp.Validate(100000)
			Expect(err).To(MatchError(ContainSubstring(`Unknown postgresql privilege target_type: foo`)))
		})

		It("returns an error if no target_name supplied with TABLE target_type", func() {
			bp := PostgresUserBindParameters {
				IsOwner: boolPointer(false),
				DefaultPrivilegePolicy: "grant",
				RevokePrivileges: &[]PostgresqlPrivilege{
					PostgresqlPrivilege{
						TargetType: "table",
						Privilege: "ALL",
					},
				},
			}

			err := bp.Validate(100000)
			Expect(err).To(MatchError(ContainSubstring(`Must provide a non-empty target_name for 'TABLE' postgresql privilege target_type`)))
		})

		It("returns an error if an invalid target_name or schema_name is supplied with TABLE target_type", func() {
			bp := PostgresUserBindParameters {
				IsOwner: boolPointer(false),
				DefaultPrivilegePolicy: "grant",
				RevokePrivileges: &[]PostgresqlPrivilege{
					PostgresqlPrivilege{
						TargetType: "table",
						TargetName: stringPointer("bar ✈"),
						Privilege: "ALL",
					},
				},
			}

			err := bp.Validate(100000)
			Expect(err).To(MatchError(ContainSubstring(`Non-ASCII characters in postgresql object names not (yet) supported`)))

			bp = PostgresUserBindParameters {
				IsOwner: boolPointer(false),
				DefaultPrivilegePolicy: "grant",
				RevokePrivileges: &[]PostgresqlPrivilege{
					PostgresqlPrivilege{
						TargetType: "table",
						TargetSchema: stringPointer("in✈valid"),
						TargetName: stringPointer("something;valid"),
						Privilege: "ALL",
					},
				},
			}

			err = bp.Validate(100000)
			Expect(err).To(MatchError(ContainSubstring(`Non-ASCII characters in postgresql object names not (yet) supported`)))
		})

		It("returns an error if invalid column_names are supplied with TABLE target_type", func() {
			bp := PostgresUserBindParameters {
				IsOwner: boolPointer(false),
				DefaultPrivilegePolicy: "revoke",
				GrantPrivileges: &[]PostgresqlPrivilege{
					PostgresqlPrivilege{
						TargetType: "TABLE",
						TargetName: stringPointer("some_table"),
						Privilege: "select",
						ColumnNames: &[]string{
							"valid 123",
							"✈",
						},
					},
				},
			}

			err := bp.Validate(100000)
			Expect(err).To(MatchError(ContainSubstring(`Non-ASCII characters in postgresql object names not (yet) supported: ✈`)))
		})

		It("returns an error if invalid column_names are supplied with TABLE target_type", func() {
			bp := PostgresUserBindParameters {
				IsOwner: boolPointer(false),
				DefaultPrivilegePolicy: "revoke",
				GrantPrivileges: &[]PostgresqlPrivilege{
					PostgresqlPrivilege{
						TargetType: "TABLE",
						TargetName: stringPointer("some_table"),
						Privilege: "delete",
						ColumnNames: &[]string{
							"valid 123",
						},
					},
				},
			}

			err := bp.Validate(90501)
			Expect(err).To(MatchError(ContainSubstring(`Unknown postgresql column privilege: delete`)))
		})

		It("returns an error if an invalid target_name or schema_name is supplied with TABLE target_type", func() {
			bp := PostgresUserBindParameters {
				IsOwner: boolPointer(false),
				DefaultPrivilegePolicy: "grant",
				RevokePrivileges: &[]PostgresqlPrivilege{
					PostgresqlPrivilege{
						TargetType: "SEQUENCE",
						TargetSchema: stringPointer("invalid✈"),
						TargetName: stringPointer("bar"),
						Privilege: "ALL",
					},
				},
			}

			err := bp.Validate(100000)
			Expect(err).To(MatchError(ContainSubstring(`Non-ASCII characters in postgresql object names not (yet) supported: invalid✈`)))

			bp = PostgresUserBindParameters {
				IsOwner: boolPointer(false),
				DefaultPrivilegePolicy: "grant",
				RevokePrivileges: &[]PostgresqlPrivilege{
					PostgresqlPrivilege{
						TargetType: "SEQUENCE",
						TargetName: stringPointer("bar"),
						Privilege: "ALL",
						ColumnNames: &[]string{
							"valid 123",
						},
					},
				},
			}

			err = bp.Validate(100000)
			Expect(err).To(MatchError(ContainSubstring(`column_names makes no sense for 'SEQUENCE' postgresql privilege target_type`)))
		})

		It("returns an error if an inappropriate privilege is specified with SEQUENCE target_type", func() {
			bp := PostgresUserBindParameters {
				IsOwner: boolPointer(false),
				DefaultPrivilegePolicy: "revoke",
				GrantPrivileges: &[]PostgresqlPrivilege{
					PostgresqlPrivilege{
						TargetType: "SEQUENCE",
						TargetName: stringPointer("bar"),
						Privilege: "INSERT",
					},
				},
			}

			err := bp.Validate(100000)
			Expect(err).To(MatchError(ContainSubstring(`Unknown postgresql sequence privilege: INSERT`)))
		})

		It("returns an error if inappropriate options are specified with DATABASE target_type", func() {
			bp := PostgresUserBindParameters {
				IsOwner: boolPointer(false),
				DefaultPrivilegePolicy: "grant",
				RevokePrivileges: &[]PostgresqlPrivilege{
					PostgresqlPrivilege{
						TargetType: "database",
						TargetName: stringPointer("bar"),
						Privilege: "ALL",
					},
				},
			}

			err := bp.Validate(100000)
			Expect(err).To(MatchError(ContainSubstring(`target_name makes no sense for 'DATABASE' postgresql privilege target_type`)))

			bp = PostgresUserBindParameters {
				IsOwner: boolPointer(false),
				DefaultPrivilegePolicy: "grant",
				RevokePrivileges: &[]PostgresqlPrivilege{
					PostgresqlPrivilege{
						TargetType: "DATABASE",
						TargetSchema: stringPointer("foo123"),
						Privilege: "ALL",
					},
				},
			}

			err = bp.Validate(100000)
			Expect(err).To(MatchError(ContainSubstring(`target_schema makes no sense for 'DATABASE' postgresql privilege target_type`)))

			bp = PostgresUserBindParameters {
				IsOwner: boolPointer(false),
				DefaultPrivilegePolicy: "revoke",
				GrantPrivileges: &[]PostgresqlPrivilege{
					PostgresqlPrivilege{
						TargetType: "DATABASE",
						Privilege: "ALL",
						ColumnNames: &[]string{
							"valid 123",
						},
					},
				},
			}

			err = bp.Validate(90501)
			Expect(err).To(MatchError(ContainSubstring(`column_names makes no sense for 'DATABASE' postgresql privilege target_type`)))
		})

		It("returns an error if an inappropriate privilege is specified with DATABASE target_type", func() {
			bp := PostgresUserBindParameters {
				IsOwner: boolPointer(false),
				DefaultPrivilegePolicy: "revoke",
				GrantPrivileges: &[]PostgresqlPrivilege{
					PostgresqlPrivilege{
						TargetType: "DATABASE",
						Privilege: "INSERT",
					},
				},
			}

			err := bp.Validate(100000)
			Expect(err).To(MatchError(ContainSubstring(`Unknown postgresql database privilege: INSERT`)))
		})

		It("returns an error if no target_name supplied with SCHEMA target_type", func() {
			bp := PostgresUserBindParameters {
				IsOwner: boolPointer(false),
				DefaultPrivilegePolicy: "grant",
				RevokePrivileges: &[]PostgresqlPrivilege{
					PostgresqlPrivilege{
						TargetType: "schema",
						Privilege: "all",
					},
				},
			}

			err := bp.Validate(100000)
			Expect(err).To(MatchError(ContainSubstring(`Must provide a non-empty target_name for 'SCHEMA' postgresql privilege target_type`)))
		})

		It("returns an error if invalid target_name supplied with SCHEMA target_type", func() {
			bp := PostgresUserBindParameters {
				IsOwner: boolPointer(false),
				DefaultPrivilegePolicy: "grant",
				RevokePrivileges: &[]PostgresqlPrivilege{
					PostgresqlPrivilege{
						TargetType: "schema",
						TargetName: stringPointer("invalid✈"),
						Privilege: "all",
					},
				},
			}

			err := bp.Validate(100000)
			Expect(err).To(MatchError(ContainSubstring(`Non-ASCII characters in postgresql object names not (yet) supported: invalid✈`)))
		})

		It("returns an error if inappropriate options are specified with SCHEMA target_type", func() {
			bp := PostgresUserBindParameters {
				IsOwner: boolPointer(false),
				DefaultPrivilegePolicy: "grant",
				RevokePrivileges: &[]PostgresqlPrivilege{
					PostgresqlPrivilege{
						TargetType: "SCHEMA",
						TargetSchema: stringPointer("foo"),
						TargetName: stringPointer("bar"),
						Privilege: "ALL",
					},
				},
			}

			err := bp.Validate(100000)
			Expect(err).To(MatchError(ContainSubstring(`target_schema makes no sense for 'SCHEMA' postgresql privilege target_type (try target_name instead)`)))

			bp = PostgresUserBindParameters {
				IsOwner: boolPointer(false),
				DefaultPrivilegePolicy: "grant",
				RevokePrivileges: &[]PostgresqlPrivilege{
					PostgresqlPrivilege{
						TargetType: "SCHEMA",
						TargetName: stringPointer("bar"),
						Privilege: "ALL",
						ColumnNames: &[]string{
							"valid 123",
						},
					},
				},
			}

			err = bp.Validate(100000)
			Expect(err).To(MatchError(ContainSubstring(`column_names makes no sense for 'SCHEMA' postgresql privilege target_type`)))
		})

		It("returns an error if an inappropriate privilege is specified with SCHEMA target_type", func() {
			bp := PostgresUserBindParameters {
				IsOwner: boolPointer(false),
				DefaultPrivilegePolicy: "revoke",
				GrantPrivileges: &[]PostgresqlPrivilege{
					PostgresqlPrivilege{
						TargetType: "schema",
						TargetName: stringPointer("bar"),
						Privilege: "EXECUTE",
					},
				},
			}

			err := bp.Validate(90501)
			Expect(err).To(MatchError(ContainSubstring(`Unknown postgresql schema privilege: EXECUTE`)))
		})
	})

	Describe("PlPgSQL generation", func() {
		It("generates a correct default privilege statement for default-revoke policy", func() {
			bp := PostgresUserBindParameters {
				IsOwner: boolPointer(false),
				DefaultPrivilegePolicy: "REVOKE",
			}

			Expect(bp.Validate(100000)).ToNot(HaveOccurred())

			defaultPlPgSQL := bp.GetDefaultPrivilegePlPgSQL("someuser", "somedb")

			Expect(defaultPlPgSQL).To(Equal(`
	DECLARE
		username text := '"someuser"';
		dbname text := '"somedb"';
		r RECORD;
	BEGIN
		EXECUTE 'GRANT CONNECT ON DATABASE ' || dbname || ' TO ' || username;
	END`))
		})

		It("generates a correct default privilege statement for default-grant policy", func() {
			bp := PostgresUserBindParameters {
				IsOwner: boolPointer(false),
				DefaultPrivilegePolicy: "GRANT",
			}

			Expect(bp.Validate(100000)).ToNot(HaveOccurred())

			defaultPlPgSQL := bp.GetDefaultPrivilegePlPgSQL("someuser", "somedb")

			Expect(defaultPlPgSQL).To(Equal(`
	DECLARE
		username text := '"someuser"';
		dbname text := '"somedb"';
		r RECORD;
	BEGIN
		FOR r IN SELECT schema_name FROM information_schema.schemata WHERE schema_name != 'information_schema' AND schema_name NOT LIKE 'pg_%' LOOP
			EXECUTE 'GRANT ALL ON ALL TABLES IN SCHEMA ' || quote_ident(r.schema_name) || ' TO ' || username;
			EXECUTE 'GRANT ALL ON ALL SEQUENCES IN SCHEMA ' || quote_ident(r.schema_name) || ' TO ' || username;
			EXECUTE 'GRANT ALL ON SCHEMA ' || quote_ident(r.schema_name) || ' TO ' || username;
		END LOOP;

		EXECUTE 'GRANT ALL ON DATABASE ' || dbname || ' TO ' || username;

		EXECUTE 'ALTER DEFAULT PRIVILEGES GRANT ALL ON TABLES TO ' || username;
		EXECUTE 'ALTER DEFAULT PRIVILEGES GRANT ALL ON SEQUENCES TO ' || username;
		EXECUTE 'ALTER DEFAULT PRIVILEGES GRANT ALL ON SCHEMAS TO ' || username;

		EXECUTE 'GRANT CONNECT ON DATABASE ' || dbname || ' TO ' || username;
	END`))
		})

		It("Generates a correct privilege assignment statement for column-targeted policies", func() {
			bp := PostgresUserBindParameters {
				IsOwner: boolPointer(false),
				DefaultPrivilegePolicy: "REVOKE",
				GrantPrivileges: &[]PostgresqlPrivilege{
					PostgresqlPrivilege{
						TargetType: "table",
						TargetName: stringPointer("Some Name"),
						TargetSchema: stringPointer("a-schema"),
						Privilege: "SELECT",
						ColumnNames: &[]string{
							"foo",
							"bar",
						},
					},
				},
			}

			Expect(bp.Validate(100000)).ToNot(HaveOccurred())

			assnPlPgSQL := bp.GetPrivilegeAssignmentPlPgSQL("someuser", "somedb")

			Expect(assnPlPgSQL).To(Equal(`
	DECLARE
		username text := '"someuser"';
		dbname text := '"somedb"';
		r RECORD;
	BEGIN
		BEGIN
			EXECUTE 'GRANT SELECT (' || '"foo", "bar"' || ') ON TABLE ' || '"a-schema"."Some Name"' || ' TO ' || username;
		EXCEPTION
			WHEN undefined_column OR undefined_table OR invalid_schema_name THEN
				NULL;
		END;
	END`))
		})

		It("Generates a correct privilege assignment statement for table-targeted policies", func() {
			bp := PostgresUserBindParameters {
				IsOwner: boolPointer(false),
				DefaultPrivilegePolicy: "GRANT",
				RevokePrivileges: &[]PostgresqlPrivilege{
					PostgresqlPrivilege{
						TargetType: "table",
						TargetName: stringPointer("Some Name"),
						Privilege: "DELETE",
						ColumnNames: &[]string{},
					},
				},
			}

			Expect(bp.Validate(100000)).ToNot(HaveOccurred())

			assnPlPgSQL := bp.GetPrivilegeAssignmentPlPgSQL("someuser", "somedb")

			Expect(assnPlPgSQL).To(Equal(`
	DECLARE
		username text := '"someuser"';
		dbname text := '"somedb"';
		r RECORD;
	BEGIN
		BEGIN
			EXECUTE 'REVOKE DELETE ON TABLE ' || '"Some Name"' || ' FROM ' || username;
		EXCEPTION
			WHEN undefined_column OR undefined_table OR invalid_schema_name THEN
				NULL;
		END;
	END`))
		})

		It("Generates a correct privilege assignment statement for database-targeted policies", func() {
			bp := PostgresUserBindParameters {
				IsOwner: boolPointer(false),
				DefaultPrivilegePolicy: "GRANT",
				RevokePrivileges: &[]PostgresqlPrivilege{
					PostgresqlPrivilege{
						TargetType: "DATABASE",
						Privilege: "TEMP",
					},
				},
			}

			Expect(bp.Validate(100000)).ToNot(HaveOccurred())

			assnPlPgSQL := bp.GetPrivilegeAssignmentPlPgSQL("someuser", "somedb")

			Expect(assnPlPgSQL).To(Equal(`
	DECLARE
		username text := '"someuser"';
		dbname text := '"somedb"';
		r RECORD;
	BEGIN
		BEGIN
			EXECUTE 'REVOKE TEMP ON DATABASE ' || dbname || ' FROM ' || username;
		EXCEPTION
			WHEN undefined_column OR undefined_table OR invalid_schema_name THEN
				NULL;
		END;
	END`))
		})

		It("Generates a correct privilege assignment statement for schema-targeted policies", func() {
			bp := PostgresUserBindParameters {
				IsOwner: boolPointer(false),
				DefaultPrivilegePolicy: "revoke",
				GrantPrivileges: &[]PostgresqlPrivilege{
					PostgresqlPrivilege{
						TargetType: "Schema",
						Privilege: "USAGE",
						TargetName: stringPointer("abc123"),
					},
				},
			}

			Expect(bp.Validate(100000)).ToNot(HaveOccurred())

			assnPlPgSQL := bp.GetPrivilegeAssignmentPlPgSQL("someuser", "somedb")

			Expect(assnPlPgSQL).To(Equal(`
	DECLARE
		username text := '"someuser"';
		dbname text := '"somedb"';
		r RECORD;
	BEGIN
		BEGIN
			EXECUTE 'GRANT USAGE ON SCHEMA ' || '"abc123"' || ' TO ' || username;
		EXCEPTION
			WHEN undefined_column OR undefined_table OR invalid_schema_name THEN
				NULL;
		END;
	END`))
		})

		It("Generates a correct privilege assignment statement for sequence-targeted policies", func() {
			bp := PostgresUserBindParameters {
				IsOwner: boolPointer(false),
				DefaultPrivilegePolicy: "revoke",
				GrantPrivileges: &[]PostgresqlPrivilege{
					PostgresqlPrivilege{
						TargetType: "sequence",
						Privilege: "ALL",
						TargetSchema: stringPointer("Some Schema"),
						TargetName: stringPointer("abc123"),
					},
				},
			}

			Expect(bp.Validate(100000)).ToNot(HaveOccurred())

			assnPlPgSQL := bp.GetPrivilegeAssignmentPlPgSQL("someuser", "somedb")

			Expect(assnPlPgSQL).To(Equal(`
	DECLARE
		username text := '"someuser"';
		dbname text := '"somedb"';
		r RECORD;
	BEGIN
		BEGIN
			EXECUTE 'GRANT ALL ON SEQUENCE ' || '"Some Schema"."abc123"' || ' TO ' || username;
		EXCEPTION
			WHEN undefined_column OR undefined_table OR invalid_schema_name THEN
				NULL;
		END;
	END`))
		})

		It("Generates a correct privilege assignment statement for multi-clause policies", func() {
			bp := PostgresUserBindParameters {
				IsOwner: boolPointer(false),
				DefaultPrivilegePolicy: "grant",
				RevokePrivileges: &[]PostgresqlPrivilege{
					PostgresqlPrivilege{
						TargetType: "table",
						TargetName: stringPointer("Some Name"),
						TargetSchema: stringPointer("a-schema"),
						Privilege: "UPDATE",
						ColumnNames: &[]string{
							"foo",
							"bar",
							"b a z",
						},
					},
					PostgresqlPrivilege{
						TargetType: "database",
						Privilege: "all",
					},
					PostgresqlPrivilege{
						TargetType: "TABLE",
						TargetName: stringPointer("Some Name"),
						TargetSchema: stringPointer("a-schema"),
						Privilege: "SELECT",
						ColumnNames: &[]string{
							"qux",
						},
					},
					PostgresqlPrivilege{
						TargetType: "sequence",
						Privilege: "USAGE",
						TargetName: stringPointer("abc123"),
					},
				},
			}

			Expect(bp.Validate(100000)).ToNot(HaveOccurred())

			assnPlPgSQL := bp.GetPrivilegeAssignmentPlPgSQL("someuser", "somedb")

			Expect(assnPlPgSQL).To(Equal(`
	DECLARE
		username text := '"someuser"';
		dbname text := '"somedb"';
		r RECORD;
	BEGIN
		BEGIN
			EXECUTE 'REVOKE UPDATE (' || '"foo", "bar", "b a z"' || ') ON TABLE ' || '"a-schema"."Some Name"' || ' FROM ' || username;
		EXCEPTION
			WHEN undefined_column OR undefined_table OR invalid_schema_name THEN
				NULL;
		END;
		BEGIN
			EXECUTE 'REVOKE ALL ON DATABASE ' || dbname || ' FROM ' || username;
		EXCEPTION
			WHEN undefined_column OR undefined_table OR invalid_schema_name THEN
				NULL;
		END;
		BEGIN
			EXECUTE 'REVOKE SELECT (' || '"qux"' || ') ON TABLE ' || '"a-schema"."Some Name"' || ' FROM ' || username;
		EXCEPTION
			WHEN undefined_column OR undefined_table OR invalid_schema_name THEN
				NULL;
		END;
		BEGIN
			EXECUTE 'REVOKE USAGE ON SEQUENCE ' || '"abc123"' || ' FROM ' || username;
		EXCEPTION
			WHEN undefined_column OR undefined_table OR invalid_schema_name THEN
				NULL;
		END;
	END`))
		})

		It("Generates an empty privilege assignment statement for privilege-less policies", func() {
			bp := PostgresUserBindParameters {
				IsOwner: boolPointer(false),
				DefaultPrivilegePolicy: "grant",
			}

			Expect(bp.Validate(100000)).ToNot(HaveOccurred())

			assnPlPgSQL := bp.GetPrivilegeAssignmentPlPgSQL("someuser", "somedb")

			Expect(assnPlPgSQL).To(Equal(""))
		})
	})
})
