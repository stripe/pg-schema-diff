package pgengine

import (
	"database/sql"
	"fmt"

	_ "github.com/jackc/pgx/v4/stdlib"
)

type DB struct {
	connOpts ConnectionOptions

	dropped bool
}

func (d *DB) GetName() string {
	return d.connOpts[ConnectionOptionDatabase]
}

func (d *DB) GetConnOpts() ConnectionOptions {
	return d.connOpts
}

func (d *DB) GetDSN() string {
	return d.GetConnOpts().ToDSN()
}

// DropDB drops the database
func (d *DB) DropDB() error {
	if d.dropped {
		return nil
	}

	// Use the pgDsn as we are dropping the test database
	db, err := sql.Open("pgx", d.GetConnOpts().With(ConnectionOptionDatabase, "postgres").ToDSN())
	if err != nil {
		return err
	}
	defer db.Close()

	// Disallow further connections to the test database, except for superusers
	_, err = db.Exec(fmt.Sprintf("ALTER DATABASE \"%s\" CONNECTION LIMIT 0", d.GetName()))
	if err != nil {
		return err
	}

	// Drop existing connections, so that we can drop the table
	_, err = db.Exec("SELECT PG_TERMINATE_BACKEND(pid) FROM pg_stat_activity WHERE datname = $1", d.GetName())
	if err != nil {
		return err
	}

	// Finally, drop the table
	_, err = db.Exec(fmt.Sprintf("DROP DATABASE \"%s\"", d.GetName()))
	if err != nil {
		return err
	}

	d.dropped = true
	return nil
}
