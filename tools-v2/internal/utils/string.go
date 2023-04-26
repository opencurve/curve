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
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
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
	PATH_REGEX    = `^(/[^/ ]*)+/?$`
	FS_NAME_REGEX = "^([a-z0-9]+\\-?)+$"
	K_STRING_TRUE = "true"

	ROOT_PATH       = "/"
	RECYCLEBIN_PATH = "/RecycleBin"
)

func IsValidAddr(addr string) bool {
	matched, err := regexp.MatchString(IP_PORT_REGEX, addr)
	if err != nil || !matched {
		return false
	}
	return true
}

func IsValidFsname(fsName string) bool {
	matched, err := regexp.MatchString(FS_NAME_REGEX, fsName)
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

func GetString2Signature(date uint64, owner string) string {
	return fmt.Sprintf("%d:%s", date, owner)
}

func CalcString2Signature(in string, secretKet string) string {
	h := hmac.New(sha256.New, []byte(secretKet))
	h.Write([]byte(in))
	return base64.StdEncoding.EncodeToString(h.Sum(nil))
}

func IsDigit(r rune) bool {
	return '0' <= r && r <= '9'
}

func IsAlpha(r rune) bool {
	return ('a' <= r && r <= 'z') || IsUpper(r)
}

func IsUpper(r rune) bool {
	return 'A' <= r && r <= 'Z'
}

func ToUnderscoredName(src string) string {
	var ret string
	for i, c := range src {
		if IsAlpha(c) {
			if c < 'a' { // upper cases
				if i != 0 && !IsUpper(rune(src[i-1])) && ret[len(ret)-1] != '-' {
					ret += "_"
				}
				ret += string(c - 'A' + 'a')
			} else {
				ret += string(c)
			}
		} else if IsDigit(c) {
			ret += string(c)
		} else if len(ret) == 0 || ret[len(ret)-1] != '_' {
			ret += "_"
		}
	}
	return ret
}
<<<<<<< HEAD
=======

func Addr2IpPort(addr string) (string, uint32, *cmderror.CmdError) {
	ipPort := strings.Split(addr, ":")
	if len(ipPort) != 2 {
		err := cmderror.ErrGetAddr()
		err.Format("server", addr)
		return "", 0, err
	}
	u64Port, err := strconv.ParseUint(ipPort[1], 10, 32)
	if err != nil {
		pErr := cmderror.ErrGetAddr()
		pErr.Format("server", addr)
		return "", 0, pErr
	}
	return ipPort[0], uint32(u64Port), cmderror.Success()
}

func StringList2Uint64List(strList []string) ([]uint64, error) {
	retList := make([]uint64, 0)
	for _, str := range strList {
		v, err := strconv.ParseUint(str, 10, 64)
		if err != nil {
			return nil, err
		}
		retList = append(retList, v)
	}
	return retList, nil
}
>>>>>>> a7135515... [feat]tools-v2: add bs scan status
