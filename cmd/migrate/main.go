package main

import "github.com/smrz2001/go-cas/services/migrate"

func main() {
	migrate.NewMigrationService().Migrate()
}
