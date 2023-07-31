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
package cobratemplate

import (
	"bytes"
	"fmt"
	"text/template"

	"github.com/docker/cli/cli"
	"github.com/docker/cli/cli/command"
	"github.com/moby/term"
	"github.com/opencurve/curve/tools-v2/pkg/config"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
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

{{- if .HasAvailableLocalFlags}}

Flags:
{{ wrapLocalFlagUsages . | trimRightSpace}}
{{- end}}
{{- if .HasAvailableInheritedFlags}}

Global Flags:
{{ wrapInheritedFlagUsages . | trimRightSpace}}
{{- end}}
{{- if .HasExample}}

Examples:
{{ .Example }}

{{ else if not .HasSubCommands}}

Examples:
{{ genExample .}}

{{- end}}

{{- if .HasSubCommands }}

Run '{{.CommandPath}} COMMAND --help' for more information on a command.
{{- end}}
`

	completionTemplate = `To load completions:

Bash:

$ source <(curve completion bash)

# To load completions for each session, execute once:
Linux:
  $ {{RootCommand .}} completion bash > /etc/bash_completion.d/{{RootCommand .}}
MacOS:
  $ {{RootCommand .}} completion bash > /usr/local/etc/bash_completion.d/{{RootCommand .}}

Zsh:

# If shell completion is not already enabled in your environment you will need
# to enable it.  You can execute the following once:

$ echo "autoload -U compinit; compinit" >> ~/.zshrc

# To load completions for each session, execute once:
$ {{RootCommand .}} completion zsh > "${fpath[1]}/_{{RootCommand .}}"

# You will need to start a new shell for this setup to take effect.

Fish:

$ {{RootCommand .}} completion fish | source

# To load completions for each session, execute once:
$ {{RootCommand .}} completion fish > ~/.config/fish/completions/{{RootCommand .}}.fish
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

// func wrappedFlagUsages(cmd *cobra.Command) string {
// 	width := 80
// 	if ws, err := term.GetWinsize(0); err == nil {
// 		width = int(ws.Width)
// 	}
// 	return cmd.Flags().FlagUsagesWrapped(width - 1)
// }

func wrapLocalFlagUsages(cmd *cobra.Command) string {
	width := 80
	if ws, err := term.GetWinsize(0); err == nil {
		width = int(ws.Width)
	}
	return cmd.LocalFlags().FlagUsagesWrapped(width - 1)
}

func wrapInheritedFlagUsages(cmd *cobra.Command) string {
	width := 80
	if ws, err := term.GetWinsize(0); err == nil {
		width = int(ws.Width)
	}
	return cmd.InheritedFlags().FlagUsagesWrapped(width - 1)
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

type cmdType int

const (
	BSNAME          = "bs"
	FSNAME          = "fs"
	Unknown cmdType = iota
	RootCmd
	BsCmd
	FsCmd
)

// return the type of command (bs or fs or root)
func GetCmdType(cmd *cobra.Command) cmdType {
	if !cmd.HasParent() {
		return RootCmd
	}
	if cmd.Parent().HasParent() {
		return GetCmdType(cmd.Parent())
	}
	switch cmd.Name() {
	case BSNAME:
		return BsCmd
	case FSNAME:
		return FsCmd
	default:
		return Unknown
	}
}

func genExample(cmd *cobra.Command) string {
	ret := cmd.CommandPath()
	if cmd.HasLocalFlags() {
		lFlags := cmd.LocalFlags()
		lFlags.VisitAll(func(flag *pflag.Flag) {
			required := flag.Annotations[cobra.BashCompOneRequiredFlag]
			if len(required) > 0 && required[0] == "true" {
				ret += fmt.Sprintf(" --%s %v", flag.Name, AvailableValueStr(flag, GetCmdType(cmd)))
			}
		})
	}
	return ret
}

func SetUsageTemplate(cmd *cobra.Command) {
	cobra.AddTemplateFunc("subCommands", subCommands)
	cobra.AddTemplateFunc("hasSubCommands", hasSubCommands)
	cobra.AddTemplateFunc("wrapLocalFlagUsages", wrapLocalFlagUsages)
	cobra.AddTemplateFunc("wrapInheritedFlagUsages", wrapInheritedFlagUsages)
	cobra.AddTemplateFunc("genExample", genExample)
	cmd.SetUsageTemplate(usageTemplate)
}

func AvailableValueStr(flag *pflag.Flag, cmdtype cmdType) string {
	switch cmdtype {
	case BsCmd:
		return config.BsAvailableValueStr(flag.Name)
	}
	return ""
}

func RootCommand(cmd *cobra.Command) string {
	root := cmd.Root()
	return root.Name()
}

func SetupCompletionCommand(completionCmd *cobra.Command) {
	buffer := bytes.NewBufferString("")
	funcMap := template.FuncMap{
		"RootCommand": RootCommand,
	}
	tmpl, err := template.New("completion").Funcs(funcMap).Parse(completionTemplate)
	if err != nil {
		cobra.CheckErr(err)
	}
	err = tmpl.Execute(buffer, completionCmd)
	if err != nil {
		cobra.CheckErr(err)
	}
	completionCmd.Long = buffer.String()

}
