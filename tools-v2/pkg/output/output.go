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
 * Created Date: 2022-05-11
 * Author: chengyi (Cyber-SiKu)
 */

package output

import (
	"encoding/json"
	"fmt"

	cmderror "github.com/opencurve/curve/tools-v2/internal/error"
	basecmd "github.com/opencurve/curve/tools-v2/pkg/cli/command"
)

type CurveCliOutput struct {
	Error  cmderror.CmdError `json:"error"`
	Result string            `json:"result"`
}

func FinalCmdOutputJsonString(finalCmd *basecmd.FinalCurveCmd) (string, error) {
	output, err := json.MarshalIndent(CurveCliOutput{
		Error:  finalCmd.Error,
		Result: finalCmd.Result,
	}, "", "	")
	if err != nil {
		return "", err
	}
	return string(output), nil
}

func FinalCmdOutputPlainString(finalCmd *basecmd.FinalCurveCmd,
	funcs basecmd.FinalCurveCmdFunc) (string, error) {
	ret, err := funcs.ResultPlainString()

	if err != nil && finalCmd.Error.Code != 0 {
		return ret, fmt.Errorf("%d\n%s", finalCmd.Error.Code, finalCmd.Error.Message)
	}
	return ret, err
}
