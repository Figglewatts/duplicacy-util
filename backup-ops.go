// Copyright © 2018 Jeff Coffler <jeff@taltos.com>
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"
)

type backupRevision struct {
	storage          string
	chunkTotalCount  string // Like: 348444
	chunkTotalSize   string // Like: 1668G
	filesTotalCount  string // Like: 161318
	filesTotalSize   string // Like: 1666G
	filesNewCount    string // Like: 373
	filesNewSize     string // Like: 15,951M
	chunkNewCount    string // Like: 2415
	chunkNewSize     string // Like: 12,391M
	chunkNewUploaded string // Like: 12,255M
	duration         string
}

type copyRevision struct {
	storageFrom     string
	storageTo       string // Like: 348444
	chunkTotalCount string // Like: 109
	chunkCopyCount  string // Like: 3
	chunkSkipCount  string // Like: 106
	duration        string
}

func performBackup() error {
	// Handle log file rotation (before any output to log file so old one doesn't get trashed)

	logMessage(nil, "Rotating log files")
	if err := rotateLogFiles(); err != nil {
		return err
	}

	// Create output log file
	file, err := os.Create(filepath.Join(globalLogDir, cmdConfig+".log"))
	if err != nil {
		logError(nil, fmt.Sprint("Error: ", err))
		return err
	}
	logger := log.New(file, "", log.Ltime)

	startTime := time.Now().UTC()

	logMessage(logger, fmt.Sprint("Beginning backup on ", time.Now().Format("01-02-2006 15:04:05")))

	// Notify all configure channels that the backup process has started
	notifyOfStart()

	// Perform "duplicacy backup" if required
	if cmdBackup {
		if err := performDuplicacyBackup(logger, []string{}); err != nil {
			return err
		}
	}

	// Perform "duplicacy copy" if required
	if cmdCopy {
		if err := performDuplicacyCopy(logger, []string{}); err != nil {
			return err
		}
	}

	// Perform "duplicacy prune" if required
	if cmdPrune {
		if err := performDuplicacyPrune(logger, []string{}); err != nil {
			return err
		}
	}

	// Perform "duplicacy check" if required
	if cmdCheck {
		if err := performDuplicacyCheck(logger, []string{}); err != nil {
			return err
		}
	}

	logger.Println("######################################################################")
	logMessage(logger, fmt.Sprint("Operations completed in ", getTimeDiffString(startTime, time.Now().UTC())))

	// Notify all configure channels that the backup process has completd
	err = notifyOfSuccess()

	return err
}

func performDuplicacyBackup(logger *log.Logger, testArgs []string) error {
	// Handling when processing output from "duplicacy backup" command
	var backupEntry backupRevision

	backupLogger := func(line string) {
		switch {
		// Files: 161318 total, 1666G bytes; 373 new, 15,951M bytes
		case strings.HasPrefix(line, "Files:"):
			logger.Println(line)
			logMessage(logger, fmt.Sprint("  ", line))

			// Save chunk data for inclusion into HTML portion of E-Mail message
			re := regexp.MustCompile(`.*: (\S+) total, (\S+) bytes; (\S+) new, (\S+) bytes`)
			elements := re.FindStringSubmatch(line)
			if len(elements) >= 4 {
				backupEntry.filesTotalCount = elements[1]
				backupEntry.filesTotalSize = elements[2]
				backupEntry.filesNewCount = elements[3]
				backupEntry.filesNewSize = elements[4]
			}

		// All chunks: 348444 total, 1668G bytes; 2415 new, 12,391M bytes, 12,255M bytes uploaded
		case strings.HasPrefix(line, "All chunks:"):
			logger.Println(line)
			logMessage(logger, fmt.Sprint("  ", line))

			// Save chunk data for inclusion into HTML portion of E-Mail message
			re := regexp.MustCompile(`.*: (\S+) total, (\S+) bytes; (\S+) new, (\S+) bytes, (\S+) bytes uploaded`)
			elements := re.FindStringSubmatch(line)
			if len(elements) >= 6 {
				backupEntry.chunkTotalCount = elements[1]
				backupEntry.chunkTotalSize = elements[2]
				backupEntry.chunkNewCount = elements[3]
				backupEntry.chunkNewSize = elements[4]
				backupEntry.chunkNewUploaded = elements[5]
			}

		// Try to catch and point out password problems within dupliacy
		case strings.HasPrefix(line, "Enter storage password:") || strings.HasSuffix(line, "Authorization failure"):
			logMessage(logger, "  Error: Duplicacy appears to be prompting for a password")

			logger.Println(line)
			logMessage(logger, fmt.Sprint("  ", line))

		default:
			logger.Println(line)
		}
	}

	// Perform backup operation
	for i, backupInfo := range configFile.BackupInfo {
		backupStartTime := time.Now().UTC()
		logger.Println("######################################################################")

		// Minor support for unit tests - distasteful but only reasonable option
		cmdArgs := make([]string, len(testArgs))
		copy(cmdArgs, testArgs)
		if len(cmdArgs) > 0 && cmdArgs[0] == "testbackup" {
			cmdArgs[1] = testArgs[1] + "_backup" + strconv.Itoa(i+1)
		}

		// Build remainder of command arguments
		cmdArgs = append(cmdArgs, "backup", "-storage", backupInfo.Name, "-stats")

		// Handle optional parameters that may be specified
		fmt.Printf("BACKUP: %v\n", backupInfo)
		threadCount := backupInfo.Threads
		cmdArgs = append(cmdArgs, "-threads", threadCount)

		vssFlags := ""
		if backupInfo.Vss {
			cmdArgs = append(cmdArgs, "-vss")
			vssFlags += " -vss"

			if backupInfo.VssTimeout != "" {
				cmdArgs = append(cmdArgs, "-vss-timeout", backupInfo.VssTimeout)
				vssFlags += fmt.Sprintf(" -vss-timeout %v", backupInfo.VssTimeout)
			}
		}

		quoteFlags := backupInfo.Quote
		if backupInfo.Quote != "" {
			cmdArgs = append(cmdArgs, " " + backupInfo.Quote)
		}

		logMessage(logger, fmt.Sprintf("Backing up to storage %s%s with %s threads%s", backupInfo.Name, vssFlags, threadCount, quoteFlags))

		// Execute duplicacy
		if debugFlag {
			logMessage(logger, fmt.Sprint("Executing: ", duplicacyPath, cmdArgs))
		}
		err := executor(duplicacyPath, cmdArgs, configFile.RepoDir, backupLogger)
		if err != nil {
			logError(logger, fmt.Sprint("Error executing command: ", err))
			return err
		}
		backupDuration := getTimeDiffString(backupStartTime, time.Now().UTC())

		// For test, could do a regexp on results, but easier to force known duration here
		if cmdArgs[0] == "testbackup" {
			backupDuration = "x seconds"
		}
		logMessage(logger, fmt.Sprint("  Duration: ", backupDuration))

		// Save data from backup for HTML table in E-Mail
		backupEntry.storage = backupInfo.Name
		backupEntry.duration = backupDuration
		backupTable = append(backupTable, backupEntry)
	}

	return nil
}

func performDuplicacyCopy(logger *log.Logger, testArgs []string) error {
	// Handling when processing output from "duplicacy backup" command
	var copyEntry copyRevision

	copyLogger := func(line string) {
		switch {
		// Copy complete, 107 total chunks, 0 chunks copied, 107 skipped
		case strings.HasPrefix(line, "Copy complete, "):
			logger.Println(line)
			logMessage(logger, fmt.Sprint("  ", line))

			// Save chunk data for inclusion into HTML portion of E-Mail message
			re := regexp.MustCompile(`Copy complete, (\S+) total chunks, (\S+) chunks copied, (\S+) skipped`)
			elements := re.FindStringSubmatch(line)
			if len(elements) >= 4 {
				copyEntry.chunkTotalCount = elements[1]
				copyEntry.chunkCopyCount = elements[2]
				copyEntry.chunkSkipCount = elements[3]
			}

		default:
			logger.Println(line)
		}
	}

	for i, copyInfo := range configFile.CopyInfo {
		copyStartTime := time.Now().UTC()
		logger.Println("######################################################################")

		// Minor support for unit tests - distasteful but only reasonable option
		cmdArgs := make([]string, len(testArgs))
		copy(cmdArgs, testArgs)
		if len(cmdArgs) > 0 && cmdArgs[0] == "testbackup" {
			cmdArgs[1] = testArgs[1] + "_copy" + strconv.Itoa(i+1)
		}

		// Build remainder of command arguments
		cmdArgs = append(cmdArgs, "copy", "-from", copyInfo.From, "-to", copyInfo.To)

		// Handle optional parameters that may be specified
		threadCount := copyInfo.Threads
		cmdArgs = append(cmdArgs, "-threads", threadCount)

		quoteFlags := copyInfo.Quote
		if copyInfo.Quote != "" {
			cmdArgs = append(cmdArgs, " " + copyInfo.Quote)
		}

		logMessage(logger, fmt.Sprintf("Copying from storage %s to storage %s with %s threads%s",
			copyInfo.From, copyInfo.To, threadCount, quoteFlags))

		if debugFlag {
			logMessage(logger, fmt.Sprint("Executing: ", duplicacyPath, cmdArgs))
		}
		err := executor(duplicacyPath, cmdArgs, configFile.RepoDir, copyLogger)
		if err != nil {
			logError(logger, fmt.Sprint("Error executing command: ", err))
			return err
		}
		copyDuration := getTimeDiffString(copyStartTime, time.Now().UTC())

		// For test, could do a regexp on results, but easier to force known duration here
		if cmdArgs[0] == "testbackup" {
			copyDuration = "x seconds"
		}
		logMessage(logger, fmt.Sprint("  Duration: ", copyDuration))

		// Save data from backup for HTML table in E-Mail
		copyEntry.storageFrom = copyInfo.From
		copyEntry.storageTo = copyInfo.To
		copyEntry.duration = copyDuration
		copyTable = append(copyTable, copyEntry)
	}

	return nil
}

func performDuplicacyPrune(logger *log.Logger, testArgs []string) error {
	// Handling when processing output from generic "duplicacy" command
	anon := func(s string) { logger.Println(s) }

	// Perform prune operations
	for i, pruneInfo := range configFile.PruneInfo {
		logger.Println("######################################################################")

		// Minor support for unit tests - distasteful but only reasonable option
		cmdArgs := make([]string, len(testArgs))
		copy(cmdArgs, testArgs)
		if len(cmdArgs) > 0 && cmdArgs[0] == "testbackup" {
			cmdArgs[1] = testArgs[1] + "_prune" + strconv.Itoa(i+1)
		}

		// Build remainder of command arguments
		cmdArgs = append(testArgs, "prune", "-storage", pruneInfo.Storage)
		cmdArgs = append(cmdArgs, strings.Split(pruneInfo.Keep, " ")...)

		// Handle optional parameters that may be specified
		threadCount := pruneInfo.Threads
		cmdArgs = append(cmdArgs, "-threads", threadCount)

		allFlag := ""
		if pruneInfo.All {
			allFlag = " -all"
			cmdArgs = append(cmdArgs, "-all")
		}

		quoteFlags := pruneInfo.Quote
		if pruneInfo.Quote != "" {
			cmdArgs = append(cmdArgs, " " + pruneInfo.Quote)
		}

		logMessage(logger, fmt.Sprintf("Pruning storage %s using %s thread(s)%s%s",
			pruneInfo.Storage, threadCount, allFlag, quoteFlags))

		// Execute duplicacy
		if debugFlag {
			logMessage(logger, fmt.Sprint("Executing: ", duplicacyPath, cmdArgs))
		}
		err := executor(duplicacyPath, cmdArgs, configFile.RepoDir, anon)
		if err != nil {
			logError(logger, fmt.Sprint("Error executing command: ", err))
			return err
		}
	}

	return nil
}

func performDuplicacyCheck(logger *log.Logger, testArgs []string) error {
	// Handling when processing output from generic "duplicacy" command
	anon := func(s string) { logger.Println(s) }

	// Perform check operations
	for i, checkInfo := range configFile.CheckInfo {
		logger.Println("######################################################################")

		// Minor support for unit tests - distasteful but only reasonable option
		cmdArgs := make([]string, len(testArgs))
		copy(cmdArgs, testArgs)
		if len(cmdArgs) > 0 && cmdArgs[0] == "testbackup" {
			cmdArgs[1] = testArgs[1] + "_check" + strconv.Itoa(i+1)
		}

		// Build remainder of command arguments
		cmdArgs = append(cmdArgs, "check", "-storage", checkInfo.Storage)

		// Handle optional parameters that may be specified
		allText := ""
		if checkInfo.All {
			allText = " with -all"
			cmdArgs = append(cmdArgs, "-all")
		}

		quoteFlags := checkInfo.Quote
		if checkInfo.Quote != "" {
			cmdArgs = append(cmdArgs, " " + checkInfo.Quote)
		}

		logMessage(logger, fmt.Sprintf("Checking storage %s%s%s", checkInfo.Storage, allText, quoteFlags))

		// Execute duplicacy
		if debugFlag {
			logMessage(logger, fmt.Sprint("Executing: ", duplicacyPath, cmdArgs))
		}
		err := executor(duplicacyPath, cmdArgs, configFile.RepoDir, anon)
		if err != nil {
			logError(logger, fmt.Sprint("Error executing command: ", err))
			return err
		}
	}

	return nil
}
