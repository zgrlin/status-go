package appdatabase

import (
	"database/sql"
	"errors"

	"github.com/status-im/status-go/appdatabase/migrations"
	"github.com/status-im/status-go/sqlite"
)

// InitializeDB creates db file at a given path and applies migrations.
func InitializeDB(path, password string) (*sql.DB, error) {
	db, err := sqlite.OpenDB(path, password)
	if err != nil {
		return nil, err
	}
	err = migrations.Migrate(db)
	if err != nil {
		return nil, err
	}
	return db, nil
}

// DecryptDatabase creates an unencrypted copy of the database and copies it
// over to the given directory
func DecryptDatabase(oldPath, newPath, password string) error {
	return sqlite.DecryptDB(oldPath, newPath, password)
}

// EncryptDatabase creates an encrypted copy of the database and copies it to the
// user path
func EncryptDatabase(oldPath, newPath, password string) error {
	return sqlite.EncryptDB(oldPath, newPath, password)
}

func ChangeDatabasePassword(path, password, newPassword string) error {
	return sqlite.ChangeEncryptionKey(path, password, newPassword)
}

// GetDBFilename takes an instance of sql.DB and returns the filename of the "main" database
func GetDBFilename(db *sql.DB) (string, error) {
	var i, category, filename string
	rows, err := db.Query("PRAGMA database_list;")

	defer rows.Close()
	for rows.Next() {
		err = rows.Scan(&i, &category, &filename)
		if err != nil {
			return "", err
		}

		// The "main" database is the one we care about
		if category == "main" {
			return filename, nil
		}
	}
	if err := rows.Err(); err != nil {
		return "", nil
	}

	return "", errors.New("no main database found")
}
