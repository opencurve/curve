/*
 *  Copyright (c) 2022 NetEase Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

/*
 * Project: CurveCli
 * Created Date: 2022-05-09
 * Author: chengyi (Cyber-SiKu)
 */
package cobrautil

import (
	"fmt"

	"github.com/docker/cli/cli"
	"github.com/docker/cli/cli/command"
	"github.com/moby/term"
	"github.com/spf13/cobra"
)

var (
	NoArgs            = cli.NoArgs
	RequiresMinArgs   = cli.RequiresMinArgs
	RequiresMaxArgs   = cli.RequiresMaxArgs
	RequiresRangeArgs = cli.RequiresRangeArgs
	ExactArgs         = cli.ExactArgs

	ShowHelp = command.ShowHelp
)

var (
	usageTemplate = `Usage:
{{- if not .HasSubCommands}}  {{.UseLine}}{{end}}
{{- if .HasSubCommands}}  {{ .CommandPath}} COMMAND {{- if .HasAvailableFlags}} [OPTIONS]{{end}}{{end}}

{{if ne .Long ""}}{{ .Long | trim }}{{ else }}{{ .Short | trim }}{{end}}

{{- if gt .Aliases 0}}

Aliases:
  {{.NameAndAliases}}
{{- end}}

{{- if hasSubCommands .}}

Commands:

{{- range subCommands . }}
  {{rpad .Name .NamePadding }} {{.Short}}
{{- end}}
{{- end}}

{{- if .HasAvailableFlags}}

Flags:
{{ wrappedFlagUsages . | trimRightSpace}}

{{- end}}

{{- if .HasExample}}

Examples:
{{ .Example }}

{{- end}}

{{- if .HasSubCommands }}

Run '{{.CommandPath}} COMMAND --help' for more information on a command.
{{- end}}
`
)

func subCommands(cmd *cobra.Command) []*cobra.Command {
	cmds := []*cobra.Command{}
	for _, subCmd := range cmd.Commands() {
		if subCmd.IsAvailableCommand() {
			cmds = append(cmds, subCmd)
		}
	}
	return cmds
}

func hasSubCommands(cmd *cobra.Command) bool {
	return len(subCommands(cmd)) > 0
}

func wrappedFlagUsages(cmd *cobra.Command) string {
	width := 80
	if ws, err := term.GetWinsize(0); err == nil {
		width = int(ws.Width)
	}
	return cmd.Flags().FlagUsagesWrapped(width - 1)
}

func SetFlagErrorFunc(cmd *cobra.Command) {
	cmd.SetFlagErrorFunc(func(cmd *cobra.Command, err error) error {
		if err == nil {
			return nil
		}
		return fmt.Errorf("%s\nSee '%s --help'", err, cmd.CommandPath())
	})
}

func SetHelpTemplate(cmd *cobra.Command) {
	helpTemplate := `{{if or .Runnable .HasSubCommands}}{{.UsageString}}{{end}}`
	cmd.SetHelpTemplate(helpTemplate)
}

func SetUsageTemplate(cmd *cobra.Command) {
	cobra.AddTemplateFunc("subCommands", subCommands)
	cobra.AddTemplateFunc("hasSubCommands", hasSubCommands)
	cobra.AddTemplateFunc("wrappedFlagUsages", wrappedFlagUsages)
	cmd.SetUsageTemplate(usageTemplate)
}
