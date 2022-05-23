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

import "fmt"

// It is considered here that the importance of the error is related to the
// code, and the smaller the code, the more important the error is.
// Need to ensure that the smaller the code, the more important the error is
const (
	CODE_BASE_LINE   = 10000
	CODE_SUCCESS     = 0 * CODE_BASE_LINE
	CODE_RPC_RESULT  = 1 * CODE_BASE_LINE
	CODE_HTTP_RESULT = 2 * CODE_BASE_LINE
	CODE_RPC         = 3 * CODE_BASE_LINE
	CODE_HTTP        = 4 * CODE_BASE_LINE
	CODE_INTERNAL    = 9 * CODE_BASE_LINE
	CODE_UNKNOWN     = 10 * CODE_BASE_LINE
)

type CmdError struct {
	Code    int    `json:"code"`    // exit code
	Message string `json:"message"` // exit message
}

func NewSucessCmdError() CmdError {
	return CmdError{
		Code:    CODE_SUCCESS,
		Message: "success",
	}
}

func NewInternalCmdError(code int, message string) CmdError {
	return CmdError{
		Code:    CODE_INTERNAL + code,
		Message: message,
	}
}

func NewRpcError(code int, message string) CmdError {
	return CmdError{
		Code:    CODE_RPC + code,
		Message: message,
	}
}

func NewRpcReultCmdError(code int, message string) CmdError {
	return CmdError{
		Code:    CODE_RPC_RESULT + code,
		Message: message,
	}
}

func NewHttpError(code int, message string) CmdError {
	return CmdError{
		Code:    CODE_HTTP + code,
		Message: message,
	}
}

func NewHttpResultCmdError(code int, message string) CmdError {
	return CmdError{
		Code:    CODE_HTTP_RESULT + code,
		Message: message,
	}
}

func (cmd CmdError) TypeCode() int {
	return cmd.Code / CODE_BASE_LINE * CODE_BASE_LINE
}

func (cmd CmdError) TypeName() string {
	var ret string
	switch cmd.TypeCode() {
	case CODE_SUCCESS:
		ret = "success"
	case CODE_INTERNAL:
		ret = "internal"
	case CODE_RPC:
		ret = "rpc"
	case CODE_RPC_RESULT:
		ret = "rpcResult"
	case CODE_HTTP:
		ret = "http"
	case CODE_HTTP_RESULT:
		ret = "httpResult"
	default:
		ret = "unknown"
	}
	return ret
}

func (e *CmdError) Format(args ...interface{}) {
	e.Message = fmt.Sprintf(e.Message, args...)
}

// The importance of the error is considered to be related to the code,
// please use it under the condition that the smaller the code,
// the more important the error is.
func MostImportantCmdError(err []CmdError) CmdError {
	if len(err) == 0 {
		return CmdError{
			Code:    CODE_UNKNOWN,
			Message: "unknown error",
		}
	}
	var ret CmdError
	ret.Code = CODE_UNKNOWN
	for _, e := range err {
		if e.Code < ret.Code {
			ret = e
		}
	}
	return ret
}

// keep the most important wrong id, all wrong me s sa
func MergeCmdError(err []CmdError) CmdError {
	if len(err) == 0 {
		return CmdError{
			Code:    CODE_UNKNOWN,
			Message: "unknown error",
		}
	}
	var ret CmdError
	ret.Code = CODE_UNKNOWN
	ret.Message = ""
	for _, e := range err {
		if e.Code < ret.Code {
			ret.Code = e.Code
		}
		ret.Message = ret.Message + "\n" + e.Message
	}
	ret.Message = ret.Message + "\n"
	return ret
}

var (
	ErrSuccess              = NewSucessCmdError()
	ErrHttpCreateGetRequest = NewInternalCmdError(1, "create http get request failed, the error is: %s")
	ErrDataNoExpected       = NewInternalCmdError(2, "data: %s is not as expected, the error is: %s")
	ErrHttpClient           = NewInternalCmdError(2, "http client gets error: %s")
	ErrHttpUnreadableResult = NewHttpResultCmdError(1, "http response is unreadable, the uri is: %s, the error is: %s")
	ErrHttpResultNoExpected = NewHttpResultCmdError(2, "http response is not expected, the hosts is: %s, the suburi is: %s, the result is: %s")

	ErrHttpStatus = func (statusCode int) CmdError {
		return NewHttpError(statusCode, "the url is: %s, http status code is: %d")
	}
)
