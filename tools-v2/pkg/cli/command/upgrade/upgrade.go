/*
 *  Copyright (c) 2023 NetEase Inc.
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
 * Created Date: 2023-08-05
 * Author: chengyi (Cyber-SiKu)
 */

package upgrade

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"

	cmderror "github.com/opencurve/curve/tools-v2/internal/error"
	cobrautil "github.com/opencurve/curve/tools-v2/internal/utils"
	basecmd "github.com/opencurve/curve/tools-v2/pkg/cli/command"
	"github.com/opencurve/curve/tools-v2/pkg/cli/command/version"
	"github.com/opencurve/curve/tools-v2/pkg/output"
	"github.com/spf13/cobra"
)

var (
	upgradeExample = `$ curve upgrade
$ CURVE_BASE_URL=https://curve-tool.nos-eastchina1.126.net/release CURVE_VERSION=xxxx curve upgrade`
	CURVE_VERSION         = "CURVE_VERSION"
	CURVE_BASE_URL        = "CURVE_BASE_URL"
	CURVE_DEFAULT_URL     = "https://curve-tool.nos-eastchina1.126.net/release"
	CURVE_VERSION_SUB_URL = "__version"
)

type UpgradeCommand struct {
	basecmd.FinalCurveCmd
	version string
	baseUrl string
	path    string
}

var _ basecmd.FinalCurveCmdFunc = (*UpgradeCommand)(nil) // check interface

func NewUpgradeCurveCommand() *cobra.Command {
	return NewUpgradeCommand().Cmd
}

func NewUpgradeCommand() *UpgradeCommand {
	upgradeCmd := &UpgradeCommand{
		FinalCurveCmd: basecmd.FinalCurveCmd{
			Use:     "upgrade",
			Short:   "Upgrade curve to latest version",
			Example: upgradeExample,
		},
	}

	basecmd.NewFinalCurveCli(&upgradeCmd.FinalCurveCmd, upgradeCmd)
	return upgradeCmd
}

func (uCmd *UpgradeCommand) AddFlags() {
}

func (uCmd *UpgradeCommand) Init(cmd *cobra.Command, args []string) error {
	uCmd.baseUrl = os.Getenv(CURVE_BASE_URL)
	if uCmd.baseUrl == "" {
		uCmd.baseUrl = CURVE_DEFAULT_URL
	}

	// get latest version
	uCmd.version = os.Getenv(CURVE_VERSION)
	if uCmd.version == "" {
		versionUrl := fmt.Sprintf("%s/%s", uCmd.baseUrl, CURVE_VERSION_SUB_URL)
		resp, err := http.Get(versionUrl)
		if err != nil {
			return err
		}
		defer resp.Body.Close()
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			return fmt.Errorf("get curve latest version error: %v", err)
		}
		uCmd.version = strings.TrimSpace(string(body))
	}

	if uCmd.version == version.Version {
		return fmt.Errorf("current version is up-to-date")
	}

	// determine whether the path has write permission
	path, err := os.Executable()
	if err != nil {
		return err
	}
	info, err := os.Stat(path)
	if err != nil {
		return err
	}

	if info.Mode().Perm()&(1<<(uint(7))) == 0 {
		return fmt.Errorf("no write permission on %s", path)
	}
	uCmd.path = path

	header := []string{cobrautil.ROW_RESULT}
	uCmd.SetHeader(header)
	return nil
}

func (uCmd *UpgradeCommand) Print(cmd *cobra.Command, args []string) error {
	return output.FinalCmdOutput(&uCmd.FinalCurveCmd, uCmd)
}

func (uCmd *UpgradeCommand) RunCommand(cmd *cobra.Command, args []string) error {
	downloadUrl := fmt.Sprintf("%s/curve-%s", uCmd.baseUrl, uCmd.version)
	resp, err := http.Get(downloadUrl)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	tmpFile := fmt.Sprintf("%s-%s", uCmd.path, uCmd.version)
	tmp, err := os.Create(tmpFile)
	if err != nil {
		return err
	}
	_, err = io.Copy(tmp, resp.Body)
	if err != nil {
		return err
	}

	err = os.Chmod(tmpFile, 0755)
	if err != nil {
		return err
	}
	uCmd.Error = cmderror.NewSucessCmdError()
	uCmd.Result = fmt.Sprintf("upgrade to %s success!", uCmd.version)
	row := map[string]string{
		cobrautil.ROW_RESULT: uCmd.Result.(string),
	}
	uCmd.TableNew.Append(cobrautil.Map2List(row, uCmd.Header))

	exitHandler := func(cmd *cobra.Command, args []string) error {
		// replace the old curve with the new one
		return os.Rename(tmpFile, uCmd.path)
	}
	uCmd.Cmd.PostRunE = exitHandler
	return nil
}

func (uCmd *UpgradeCommand) ResultPlainOutput() error {
	return output.FinalCmdOutputPlain(&uCmd.FinalCurveCmd)
}
