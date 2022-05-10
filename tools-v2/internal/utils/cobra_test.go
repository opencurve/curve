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
	"errors"
	"fmt"
	"testing"

	. "github.com/agiledragon/gomonkey/v2"
	"github.com/moby/term"
	. "github.com/smartystreets/goconvey/convey"
	"github.com/spf13/cobra"
)

func TestSubCommands(t *testing.T) {
	t.Parallel()
	Convey("subCommands test", t, func() {
		Convey("has subcommands", func() {
			root := &cobra.Command{
				Short: "curve",
				Run:   func(cmd *cobra.Command, args []string) {},
			}

			cmds := []*cobra.Command{}
			for i := 0; i < 10; i++ {
				tmp := &cobra.Command{
					Short: fmt.Sprintf("%d", i),
					Run:   func(cmd *cobra.Command, args []string) {},
				}
				cmds = append(cmds, tmp)
				root.AddCommand(tmp)
			}
			rootSubCmds := subCommands(root)
			So(rootSubCmds, ShouldResemble, cmds)
		})

		Convey("no SubCommand", func() {
			root := &cobra.Command{
				Short: "curve",
				Run:   func(cmd *cobra.Command, args []string) {},
			}
			rootSubCmds := subCommands(root)
			So(rootSubCmds, ShouldResemble, []*cobra.Command{})
		})
	})
}

func TestHasSubCommands(t *testing.T) {
	t.Parallel()
	Convey("hasSubcommands function test", t, func() {
		Convey("has subcommand", func() {
			root := &cobra.Command{
				Short: "curve",
				Run:   func(cmd *cobra.Command, args []string) {},
			}
			for i := 0; i < 10; i++ {
				tmp := &cobra.Command{
					Short: fmt.Sprintf("%d", i),
					Run:   func(cmd *cobra.Command, args []string) {},
				}
				root.AddCommand(tmp)
			}
			So(hasSubCommands(root), ShouldBeTrue)
		})

		Convey("testNoSubCommands", func() {
			root := &cobra.Command{
				Short: "curve",
				Run:   func(cmd *cobra.Command, args []string) {},
			}
			So(hasSubCommands(root), ShouldBeFalse)
		})
	})
}

func TestWrappedFlagUsages(t *testing.T) {
	t.Parallel()
	Convey("wrappedFlagUsages function test", t, func() {
		//  TODO(chengyi01): Not valid, please check later
		// Convey("get win size success", func() {
		// 	root := &cobra.Command{}
		// 	root.Flags().BoolP("1", "", false, "123456789123456789123456789123456789")
		// 	root.Flags().BoolP("2", "", false, "123456789123456789123456789123456789")
		// 	root.Flags().BoolP("3", "", false, "123456789123456789123456789123456789")
		// 	winsize := &term.Winsize{
		// 		Width: 2,
		// 	}
		// 	patches := ApplyFuncReturn(term.GetWinsize, winsize, nil)
		// 	defer patches.Reset()
		// 	So(wrappedFlagUsages(root), ShouldEqual, "")
		// })

		Convey("get win size fail", func() {
			root := &cobra.Command{}
			root.Flags().BoolP("", "", false, "123456789123456789123456789123456789")
			patches := ApplyFuncReturn(term.GetWinsize, nil, errors.New("no"))
			defer patches.Reset()
			So(wrappedFlagUsages(root), ShouldEqual, "      --   123456789123456789123456789123456789\n")
		})
	})
}

func TestSetHelpTemplate(t *testing.T) {
	t.Parallel()
	Convey("set UsageTemplate test", t, func() {
		root := &cobra.Command{
			Use:     "test",
			Short:   "test for UsageTemplate",
			Version: "beta",
			Run:     func(cmd *cobra.Command, args []string) {},
		}
		tmp := &cobra.Command{
			Short: "subcmd",
			Run:   func(cmd *cobra.Command, args []string) {},
		}
		root.AddCommand(tmp)

		SetHelpTemplate(root)
		rootTemplate := `Usage:
  test
  test [command]

Available Commands:
              subcmd

Use "test [command] --help" for more information about a command.
`
		So(root.UsageString(), ShouldEqual, rootTemplate)
	})
}

func TestSetUsageTemplate(t *testing.T) {
	t.Parallel()
	Convey("set UsageTemplate test", t, func() {
		root := &cobra.Command{
			Use:     "test",
			Short:   "test for UsageTemplate",
			Version: "beta",
			Run:     func(cmd *cobra.Command, args []string) {},
		}

		tmp := &cobra.Command{
			Short: "subcmd",
			Run:   func(cmd *cobra.Command, args []string) {},
		}
		root.AddCommand(tmp)

		SetUsageTemplate(root)
		rootTemplate := `Usage:  test COMMAND

test for UsageTemplate

Commands:
              subcmd

Run 'test COMMAND --help' for more information on a command.
`
		So(root.UsageString(), ShouldEqual, rootTemplate)
	})
}

func TestSetFlagErrorFunc(t *testing.T) {
	t.Parallel()
	Convey("set FlagErrorFunc test", t, func() {
		Convey("whith error: flag error", func() {
			root := &cobra.Command{
				Use:   "test",
				Short: "test for UsageTemplate",
				Run:   func(cmd *cobra.Command, args []string) {},
			}
			root.Flags().BoolP("testFlag", "", false, "testFlag")

			SetFlagErrorFunc(root)
			ret := root.FlagErrorFunc()(root, errors.New("flag error")).Error()
			expect := `flag error
See 'test --help'`
			So(ret, ShouldEqual, expect)
		})

		Convey("whith no error", func() {
			root := &cobra.Command{
				Use:   "test",
				Short: "test for UsageTemplate",
				Run:   func(cmd *cobra.Command, args []string) {},
			}
			root.Flags().BoolP("testFlag", "", false, "testFlag")

			SetFlagErrorFunc(root)
			So(root.FlagErrorFunc()(root, nil), ShouldEqual, nil)
		})
	})
}
