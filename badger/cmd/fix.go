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
	"io"
	"math"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/spf13/cobra"

	"github.com/dgraph-io/badger/v4"
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
	err = CreateIfNotExists(fof.backupDir, 0755)
	if err != nil {
		return fmt.Errorf("unable to create backup dir %s: %v", fof.backupDir, err)
	}
	err = CopyDirectory(opt.Dir, fof.backupDir)
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

func CopyDirectory(scrDir, dest string) error {
	entries, err := os.ReadDir(scrDir)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		sourcePath := filepath.Join(scrDir, entry.Name())
		destPath := filepath.Join(dest, entry.Name())

		fileInfo, err := os.Stat(sourcePath)
		if err != nil {
			return err
		}

		stat, ok := fileInfo.Sys().(*syscall.Stat_t)
		if !ok {
			return fmt.Errorf("failed to get raw syscall.Stat_t data for '%s'", sourcePath)
		}

		switch fileInfo.Mode() & os.ModeType {
		case os.ModeDir:
			if err := CreateIfNotExists(destPath, 0755); err != nil {
				return err
			}
			if err := CopyDirectory(sourcePath, destPath); err != nil {
				return err
			}
		case os.ModeSymlink:
			if err := CopySymLink(sourcePath, destPath); err != nil {
				return err
			}
		default:
			if err := Copy(sourcePath, destPath); err != nil {
				return err
			}
		}

		if err := os.Lchown(destPath, int(stat.Uid), int(stat.Gid)); err != nil {
			return err
		}

		fInfo, err := entry.Info()
		if err != nil {
			return err
		}

		isSymlink := fInfo.Mode()&os.ModeSymlink != 0
		if !isSymlink {
			if err := os.Chmod(destPath, fInfo.Mode()); err != nil {
				return err
			}
		}
	}
	return nil
}

func Copy(srcFile, dstFile string) error {
	out, err := os.Create(dstFile)
	if err != nil {
		return err
	}

	defer out.Close()

	in, err := os.Open(srcFile)
	if err != nil {
		return err
	}

	defer in.Close()

	_, err = io.Copy(out, in)
	if err != nil {
		return err
	}

	return nil
}

func Exists(filePath string) bool {
	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		return false
	}

	return true
}

func CreateIfNotExists(dir string, perm os.FileMode) error {
	if Exists(dir) {
		return nil
	}

	if err := os.MkdirAll(dir, perm); err != nil {
		return fmt.Errorf("failed to create directory: '%s', error: '%s'", dir, err.Error())
	}

	return nil
}

func CopySymLink(source, dest string) error {
	link, err := os.Readlink(source)
	if err != nil {
		return err
	}
	return os.Symlink(link, dest)
}
