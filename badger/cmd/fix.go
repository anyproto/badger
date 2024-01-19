/*
 * Copyright 2017 Dgraph Labs, Inc. and Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cmd

import (
	"fmt"
	"math"
	"os"
	"strings"
	"time"

	"github.com/dgraph-io/badger/v4"
	cp "github.com/otiai10/copy"
	"github.com/spf13/cobra"
)

var fof = struct {
	backupDir     string
	forceNotEmpty bool
}{}

// backupCmd represents the backup command
var fixCmd = &cobra.Command{
	Use:   "fix",
	Short: "Removes empty tables",
	Long:  `Removes empty tables`,
	RunE:  removeEmptyTables,
}

func init() {
	RootCmd.AddCommand(fixCmd)
	fixCmd.Flags().StringVarP(&fof.backupDir, "backup-dir", "f", "", "Folder to backup to(default is <name>_corrupted_backup)")
	fixCmd.Flags().BoolVarP(&fof.forceNotEmpty, "force-not-empty", "n", false, "Force delete not empty corrupted tables")
}

func removeEmptyTables(cmd *cobra.Command, args []string) error {
	opt := badger.DefaultOptions(sstDir).
		WithValueDir(vlogDir).
		WithNumVersionsToKeep(math.MaxInt32)

	if bo.numVersions > 0 {
		opt.NumVersionsToKeep = bo.numVersions
		opt.ReadOnly = true
	}

	// Open DB
	db, err := badger.Open(opt)
	if err == nil {
		db.Close()
		fmt.Println("Database is healthy")
		return nil
	}

	if !strings.Contains(err.Error(), "checksum") {
		return fmt.Errorf("unsupported error: %v", err)
	}

	parts := strings.Split(err.Error(), ":")
	lastPart := parts[len(parts)-1]
	path := strings.TrimSpace(lastPart)
	f, err := os.OpenFile(path, os.O_RDONLY, 0666)
	if err != nil {
		return fmt.Errorf("unable to open table file %s: %v", path, err)
	}
	_, err = f.Stat()
	if err != nil {
		return fmt.Errorf("unable to stat table file %s: %v", path, err)
	}
	allZeros := true

	for b := make([]byte, 1024); ; {
		n, err := f.Read(b)
		if err != nil {
			break
		}
		for i := 0; i < n; i++ {
			if b[i] != 0 {
				allZeros = false
				break
			}
		}
	}
	if !allZeros && !fof.forceNotEmpty {
		fmt.Println("Table is not empty. Use --force-not-empty to delete it")
		return nil
	}

	f.Close()
	fmt.Println("Database is corrupted. Trying to fix it")

	// Create backup
	if len(fof.backupDir) == 0 {
		fof.backupDir = fmt.Sprintf("%s_corrupted_backup_%d", opt.Dir, time.Now().Unix())
	}
	// copy dir recursively
	fmt.Printf("Creating backup from %s to %s\n", opt.Dir, fof.backupDir)

	err = cp.Copy(opt.Dir, fof.backupDir)
	if err != nil {
		return fmt.Errorf("unable to backup database: %v", err)
	}

	opt.DeleteCorruptedTablesFromManifest = true
	db, err = badger.Open(opt)
	if err == nil {
		db.Close()
		return fmt.Errorf("problem appear: db fixed before restart")
	}

	db, err = badger.Open(opt)
	if err != nil {
		return fmt.Errorf("unable to open database after fix attempt %v", err)
	}
	fmt.Println("Database is fixed")

	return nil
}
