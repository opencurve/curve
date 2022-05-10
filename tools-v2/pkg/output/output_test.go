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
	"testing"

	cmderror "github.com/opencurve/curve/tools-v2/internal/error"
	basecmd "github.com/opencurve/curve/tools-v2/pkg/cli/command"
	. "github.com/smartystreets/goconvey/convey"
	"github.com/spf13/cobra"
)

type testFinalCurveCmd struct {
	basecmd.FinalCurveCmd
}

func (t testFinalCurveCmd) Init(cmd *cobra.Command, args []string) error {
	return nil
}

func (t testFinalCurveCmd) RunCommand(cmd *cobra.Command, args []string) error {
	return nil
}

func (t testFinalCurveCmd) Print(cmd *cobra.Command, args []string) error {
	return nil
}

func (t testFinalCurveCmd) ResultPlainString() (string, error) {
	result := "name: curve"
	return result, nil
}

func TestFinalCmdJsonString(t *testing.T) {
	t.Parallel()
	Convey("Test FinalCmdOutputJsonString", t, func() {
		result, _ := json.Marshal(map[string]string{"name":"curve"})
		json.Unmarshal(result, &result)
		test := &testFinalCurveCmd{basecmd.FinalCurveCmd{
			Error: cmderror.CmdError{
				Code:	0,
				Message: "sucess",
			},
			Result: string(result),
		},
		}

		testString, _ := FinalCmdOutputJsonString(&test.FinalCurveCmd)
		So(testString, ShouldEqualJSON, `{"error":{"code":0,"message":"sucess"},"result":"{\"name\":\"curve\"}"}`)
	})
}

func TestFinalCmdPlainString(t *testing.T) {
	t.Parallel()
	Convey("Test FinalCmdOutputPlainString", t, func() {
		result, _ := json.Marshal(map[string]string{"name":"curve"})
		json.Unmarshal(result, &result)
		test := &testFinalCurveCmd{basecmd.FinalCurveCmd{
			Error: cmderror.CmdError{
				Code:	0,
				Message: "sucess",
			},
			Result: string(result),
		},
		}
		testString, _ := FinalCmdOutputPlainString(&test.FinalCurveCmd, test)
		So(testString, ShouldEqual, "name: curve")
	})
}
