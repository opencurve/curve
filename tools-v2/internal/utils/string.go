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
 * Created Date: 2022-05-25
 * Author: chengyi (Cyber-SiKu)
 */

package cobrautil

import (
	"bufio"
	"fmt"
	"os"
	"regexp"
	"strconv"
	"strings"

	"github.com/gookit/color"
	cmderror "github.com/opencurve/curve/tools-v2/internal/error"
)

const (
	IP_PORT_REGEX = "((\\d|[1-9]\\d|1\\d{2}|2[0-4]\\d|25[0-5])\\.(\\d|[1-9]\\d|1\\d{2}|2[0-4]\\d|25[0-5])\\.(\\d|[1-9]\\d|1\\d{2}|2[0-4]\\d|25[0-5])\\.(\\d|[1-9]\\d|1\\d{2}|2[0-4]\\d|25[0-5]):([0-9]|[1-9]\\d{1,3}|[1-5]\\d{4}|6[0-4]\\d{4}|65[0-4]\\d{2}|655[0-2]\\d|6553[0-5]))|(\\d|[1-9]\\d|1\\d{2}|2[0-4]\\d|25[0-5])\\.(\\d|[1-9]\\d|1\\d{2}|2[0-4]\\d|25[0-5])\\.(\\d|[1-9]\\d|1\\d{2}|2[0-4]\\d|25[0-5])\\.(\\d|[1-9]\\d|1\\d{2}|2[0-4]\\d|25[0-5])"

	PATH_REGEX = `^(/[^/ ]*)+/?$`
)

func IsValidAddr(addr string) bool {
	matched, err := regexp.MatchString(IP_PORT_REGEX, addr)
	if err != nil || !matched {
		return false
	}
	return true
}

// rm whitespace
func RmWitespaceStr(str string) string {
	if str == "" {
		return ""
	}

	reg := regexp.MustCompile(`\s+`)
	return reg.ReplaceAllString(str, "")
}

func prompt(prompt string) string {
	if prompt != "" {
		prompt += " "
	}
	fmt.Print(color.Yellow.Sprintf("WARNING:"), prompt)

	reader := bufio.NewReader(os.Stdin)
	input, err := reader.ReadString('\n')
	if err != nil {
		return ""
	}
	return strings.TrimSuffix(input, "\n")
}

func AskConfirmation(promptStr string, confirm string) bool {
	promptStr = promptStr + fmt.Sprintf("\nplease input [%s] to confirm:", confirm)
	ans := prompt(promptStr)
	switch strings.TrimSpace(ans) {
	case confirm:
		return true
	default:
		return false
	}
}

func IsValidPath(path string) bool {
	match, _ := regexp.MatchString(PATH_REGEX, path)
	return match
}

func SplitMountpoint(mountpoint string) ([]string, *cmderror.CmdError) {
	mountpointSlice := strings.Split(mountpoint, ":")
	if len(mountpointSlice) != 3 {
		err := cmderror.ErrSplitMountpoint()
		err.Format(mountpoint)
		return nil, err
	}
	_, errP := strconv.ParseUint(mountpointSlice[1], 10, 32)
	if errP != nil {
		err := cmderror.ErrSplitMountpoint()
		err.Format(mountpoint)
		fmt.Println(errP)
		return nil, err
	}
	return mountpointSlice, cmderror.ErrSuccess()
} 
