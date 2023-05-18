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

package cli

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	cobratemplate "github.com/opencurve/curve/tools-v2/internal/utils/template"
	"github.com/opencurve/curve/tools-v2/pkg/cli/command/curvebs"
	"github.com/opencurve/curve/tools-v2/pkg/cli/command/curvefs"
	"github.com/opencurve/curve/tools-v2/pkg/cli/command/version"
	config "github.com/opencurve/curve/tools-v2/pkg/config"
)

func addSubCommands(cmd *cobra.Command) {
	cmd.AddCommand(curvefs.NewCurveFsCommand(), curvebs.NewCurveBsCommand())
}

func setupRootCommand(cmd *cobra.Command) {
	cmd.SetVersionTemplate("curve {{.Version}}\n")
	cobratemplate.SetFlagErrorFunc(cmd)
	cobratemplate.SetHelpTemplate(cmd)
	cobratemplate.SetUsageTemplate(cmd)
}

func newCurveCommand() *cobra.Command {
	rootCmd := &cobra.Command{
		Use:     "curve fs|bs [OPTIONS] COMMAND [ARGS...]",
		Short:   "curve is a tool for managing curvefs ands curvebs",
		Version: version.Version,
		RunE: func(cmd *cobra.Command, args []string) error {
			if len(args) == 0 {
				return cobratemplate.ShowHelp(os.Stderr)(cmd, args)
			}
			return fmt.Errorf("curve: '%s' is not a curve command.\n"+
				"See 'curve --help'", args[0])
		},
		SilenceUsage: false, // silence usage when an error occurs
		CompletionOptions: cobra.CompletionOptions{
			HiddenDefaultCmd: true,
		},
	}

	rootCmd.Flags().BoolP("version", "v", false, "print curve version")
	rootCmd.PersistentFlags().BoolP("help", "h", false, "print help")
	rootCmd.PersistentFlags().StringVarP(&config.ConfPath, "conf", "c", "", "config file (default is $HOME/.curve/curve.yaml or /etc/curve/curve.yaml)")
	config.AddShowErrorPFlag(rootCmd)
	rootCmd.PersistentFlags().BoolP("verbose", "", false, "show some log")
	viper.BindPFlag("useViper", rootCmd.PersistentFlags().Lookup("viper"))

	addSubCommands(rootCmd)
	setupRootCommand(rootCmd)

	return rootCmd
}

func Execute() {
	cobra.OnInitialize(config.InitConfig)
	res := newCurveCommand().Execute()
	if res != nil {
		os.Exit(1)
	}
}
