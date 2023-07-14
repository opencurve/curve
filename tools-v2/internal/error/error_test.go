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

package cmderror

import (
	"encoding/json"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func TestCmdError(t *testing.T) {
	Convey("TestCmdErrorFormat", t, func() {
		t.Parallel()

		tmp := CmdError{
			Code:    0,
			Message: "123%s",
		}
		tmp.Format("4")
		tmp_json, err := json.Marshal(tmp)
		So(err, ShouldBeNil)

		cmp := CmdError{
			Code:    0,
			Message: "1234",
		}
		cmp_json, err := json.Marshal(cmp)
		So(err, ShouldBeNil)
		So(string(tmp_json), ShouldEqual, string(cmp_json))
	})
}
