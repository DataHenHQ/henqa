// Copyright © 2021 NAME HERE <EMAIL ADDRESS>
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

package cmd

import (
	"fmt"

	"github.com/DataHenHQ/henqa/qa"
	"github.com/spf13/cobra"
)

// validateCmd represents the validate command
var validateCmd = &cobra.Command{
	Use:   "validate",
	Short: "Validates the input data files using JSON schema files and creates reports.",
	Long: `Validates the input data files using JSON schema files and creates reports.
For example:
henqa validate file1.csv file2.csv -s schema1.json -s schema2.json
henqa validate ./dir1 ./dir2 -s schema1.jos -s schema2.json -r myreport
`,
	Args: cobra.MinimumNArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		var (
			schemas = []string{}
			outDir  = ""
			err     error
		)
		schemas, err = cmd.Flags().GetStringSlice("schema")
		if err != nil {
			fmt.Errorf("Gotten error: %v\n", err.Error())
			return
		}
		outDir, err = cmd.Flags().GetString("reports-dir")
		if err != nil {
			fmt.Errorf("Gotten error: %v\n", err.Error())
			return
		}

		err = qa.Validate(args, schemas, outDir)
		if err != nil {
			fmt.Errorf("Gotten error: %v\n", err.Error())
			return
		}
	},
}

var schemas string

func init() {
	rootCmd.AddCommand(validateCmd)
	validateCmd.Flags().StringSliceP("schema", "s", nil, "JSON schema file to use if multiple is specified, the latter will override the former")
	validateCmd.Flags().StringP("reports-dir", "r", "reports", "Reports directory that will contain the summary and detail outputs")
	validateCmd.MarkFlagRequired("schema")
}
