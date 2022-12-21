package main

import "github.com/smrz2001/go-cas/services/migration"

func main() {
	migration.NewMigration().Migrate()
}
