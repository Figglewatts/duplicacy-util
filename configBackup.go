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
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"strings"

	"github.com/creasty/defaults"
	"gopkg.in/yaml.v2"
)

var oldBackupFileFormat = false

type configurationFile struct {
	// Name (without extension) of the configuration file
	configFilename string

	// Directory for repository
	RepoDir string `yaml:"repository"`

	// Storage information for backup, copy, prune, and check commands respectively
	BackupInfo []struct {
		Name string `yaml:"name"`
		Threads string `yaml:"threads"`
		Vss bool `yaml:"vss"`
		VssTimeout string `yaml:"vssTimeout"`
		Quote string `yaml:"quote"`
	} `yaml:"storage"`
	CopyInfo []struct {
		From string `yaml:"name"`
		To string `yaml:"to"`
		Threads string `yaml:"threads"`
		Quote string `yaml:"quote"`
	} `yaml:"copy"`
	PruneInfo []struct {
		Storage string `yaml:"storage"`
		Keep string `yaml:"keep"`
		Threads string `yaml:"threads"`
		All bool `yaml:"all" default:"true"`
		Quote string `yaml:"quote"`
	} `yaml:"prune"`
	CheckInfo []struct {
		Storage string `yaml:"storage"`
		All bool `yaml:"all"`
		Quote string `yaml:"quote"`
	} `yaml:"check"`
}

func (config *configurationFile) UnmarshalYAML(unmarshal func(interface{}) error) error {
	if err := defaults.Set(config); err != nil {
		return err
	}

	type plain configurationFile
	if err := unmarshal((*plain)(config)); err != nil {
		return err
	}
	return nil
}

func newConfigurationFile() *configurationFile {
	config := new(configurationFile)
	return config
}

func (config *configurationFile) setConfig(cnfFile string) {
	config.configFilename = cnfFile
}

func (config *configurationFile) loadConfig(verboseFlag bool, debugFlag bool) error {
	var err error

	// Get the path to the config file and check it exists
	configFilePath := path.Join(globalStorageDirectory, config.configFilename)
	if _, err := os.Stat(configFilePath); os.IsNotExist(err) {
		logError(nil, fmt.Sprint("Error: ", err))
		return err
	}

	// Read the YAML file
	configFile, err := ioutil.ReadFile(configFilePath)
	if err != nil {
		logError(nil, fmt.Sprint("Error: ", err))
		return err
	}

	// Unmarshal the YAML into the configurationFile struct
	err = yaml.Unmarshal(configFile, config)
	if err != nil {
		logError(nil, fmt.Sprint("Error: ", err))
		return err
	}

	// Grab the repository location
	if config.RepoDir == "" {
		err = errors.New("missing mandatory repository location")
		logError(nil, fmt.Sprint("Error: ", err))
	}
	if _, err = os.Stat(config.RepoDir); err != nil {
		logError(nil, fmt.Sprint("Error: ", err))
	}

	// Validate, set defaults
	if len(config.BackupInfo) == 0 {
		err = errors.New("no storage locations defined in configuration")
		logError(nil, fmt.Sprint("Error: ", err))
	} else {
		for i, bi := range config.BackupInfo {
			if bi.Name == "" {
				err = fmt.Errorf("missing mandatory storage field: %d.name", i)
				logError(nil, fmt.Sprint("Error: ", err))
			}
			if bi.Threads == "" {
				config.BackupInfo[i].Threads = "1"
			}
		}
	}

	for i, ci := range config.CopyInfo {
		if ci.From == "" {
			err = fmt.Errorf("missing mandatory from field: %d.from", i)
			logError(nil, fmt.Sprint("Error: ", err))
		}
		if ci.To == "" {
			err = fmt.Errorf("missing mandatory to field: %d.to", i)
			logError(nil, fmt.Sprint("Error: ", err))
		}
		if ci.Threads == "" {
			config.CopyInfo[i].Threads = "1"
		}
	}

	if len(config.PruneInfo) == 0 {
		err = errors.New("no prune locations defined in configuration")
		logError(nil, fmt.Sprint("Error: ", err))
	}

	for i, pi := range config.PruneInfo {
		if pi.Storage == "" {
			err = fmt.Errorf("missing mandatory prune field: %d.storage", i)
			logError(nil, fmt.Sprint("Error: ", err))
		}
		if pi.Keep == "" {
			err = fmt.Errorf("missing mandatory prune field: %d.keep", i)
			logError(nil, fmt.Sprint("Error: ", err))
		} else {
			// Split/join to get "-keep " before each element
			splitList := strings.Split(pi.Keep, " ")
			for i, element := range splitList {
				splitList[i] = "-keep " + element
			}

			config.PruneInfo[i].Keep = strings.Join(splitList, " ")
		}
		if pi.Threads == "" {
			config.PruneInfo[i].Threads = "1"
		}
	}

	if len(config.CheckInfo) == 0 {
		err = errors.New("no check locations defined in configuration")
		logError(nil, fmt.Sprint("Error: ", err))
	} else {
		for i, ci := range config.CheckInfo {
			if ci.Storage == "" {
				err = fmt.Errorf("missing mandatory check field: %d.storage", i)
				logError(nil, fmt.Sprint("Error: ", err))
			}
		}
	}

	// Generate verbose/debug output if requested (assuming no fatal errors)
	if err == nil {
		logMessage(nil, fmt.Sprint("Using config file:   ", configFilePath))

		if verboseFlag {
			logMessage(nil, "")
			logMessage(nil, "Backup Information:")
			logMessage(nil, fmt.Sprintf("  Num\t%-20s%s", "Storage", "Threads"))
			for i := range config.BackupInfo {
				logMessage(nil, fmt.Sprintf("  %2d\t%-20s   %-2s", i+1, config.BackupInfo[i].Name,
					config.BackupInfo[i].Threads))
			}
			if len(config.CopyInfo) != 0 {
				logMessage(nil, "Copy Information:")
				logMessage(nil, fmt.Sprintf("  Num\t%-20s%-20s%s", "From", "To", "Threads"))
				for i := range config.CopyInfo {
					logMessage(nil, fmt.Sprintf("  %2d\t%-20s%-20s   %-2s", i+1, config.CopyInfo[i].From,
						config.CopyInfo[i].To, config.CopyInfo[i].Threads))
				}
			}
			logMessage(nil, "")

			logMessage(nil, "Prune Information:")
			for i := range config.PruneInfo {
				logMessage(nil, fmt.Sprintf("  %2d: Storage %s\n      Keep: %s", i+1, config.PruneInfo[i].Storage,
					config.PruneInfo[i].Keep))
			}
			logMessage(nil, "")

			logMessage(nil, "Check Information:")
			logMessage(nil, fmt.Sprintf("  Num\t%-20s%s", "Storage", "All Snapshots"))
			for i := range config.CheckInfo {
				logMessage(nil, fmt.Sprintf("  %2d\t%-20s    %-2s", i+1, config.CheckInfo[i].Storage,
					config.CheckInfo[i].All))
			}
			logMessage(nil, "")
		}

		if debugFlag {
			logMessage(nil, "")
			logMessage(nil, fmt.Sprint("Backup Info: ", config.BackupInfo))
			logMessage(nil, fmt.Sprint("Copy Info: ", config.CopyInfo))
			logMessage(nil, fmt.Sprint("Prune Info: ", config.PruneInfo))
			logMessage(nil, fmt.Sprint("Check Info", config.CheckInfo))
		}
	}

	return err
}