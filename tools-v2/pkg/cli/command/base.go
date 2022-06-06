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

package basecmd

import (
	"context"
	"encoding/json"
	"io/ioutil"
	"net/http"
	"strings"
	"time"

	"github.com/liushuochen/gotable/table"
	cmderror "github.com/opencurve/curve/tools-v2/internal/error"
	cobrautil "github.com/opencurve/curve/tools-v2/internal/utils"
	config "github.com/opencurve/curve/tools-v2/pkg/config"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	CURL_VERSION = "curl/7.54.0"
)

// FinalCurveCmd is the final executable command,
// it has no subcommands.
// The execution process is Init->RunCommand->Print.
// Error Use to indicate whether the command is wrong
// and the reason for the execution error
type FinalCurveCmd struct {
	Use     string            `json:"-"`
	Short   string            `json:"-"`
	Example string            `json:"-"`
	Error   cmderror.CmdError `json:"error"`
	Result  interface{}       `json:"result"`
	Table   *table.Table      `json:"-"`
	Cmd     *cobra.Command    `json:"-"`
}

// FinalCurveCmdFunc is the function type for final command
type FinalCurveCmdFunc interface {
	Init(cmd *cobra.Command, args []string) error
	RunCommand(cmd *cobra.Command, args []string) error
	Print(cmd *cobra.Command, args []string) error
	// result in plain format string
	ResultPlainOutput() error
	AddFlags()
}

// MidCurveCmd is the middle command and has subcommands.
// If you execute this command
// you will be prompted which subcommands are included
type MidCurveCmd struct {
	Use   string
	Short string
	Cmd   *cobra.Command
}

// Add subcommand for MidCurveCmd
type MidCurveCmdFunc interface {
	AddSubCommands()
}

func NewFinalCurveCli(cli *FinalCurveCmd, funcs FinalCurveCmdFunc) *cobra.Command {
	cli.Cmd = &cobra.Command{
		Use:          cli.Use,
		Short:        cli.Short,
		PreRunE:      funcs.Init,
		RunE:         funcs.RunCommand,
		PostRunE:     funcs.Print,
		SilenceUsage: false,
	}
	config.AddFormatFlag(cli.Cmd)
	funcs.AddFlags()
	return cli.Cmd
}

func NewMidCurveCli(cli *MidCurveCmd, add MidCurveCmdFunc) *cobra.Command {
	cli.Cmd = &cobra.Command{
		Use:   cli.Use,
		Short: cli.Short,
		Args:  cobrautil.NoArgs,
	}
	add.AddSubCommands()
	return cli.Cmd
}

type Metric struct {
	Addrs   []string
	SubUri  string
	timeout time.Duration
}

type MetricResult struct {
	Addr  string
	Key   string
	Value string
	Err   *cmderror.CmdError
}

func NewMetric(addrs []string, subUri string, timeout time.Duration) *Metric {
	return &Metric{
		Addrs:   addrs,
		SubUri:  subUri,
		timeout: timeout,
	}
}

func QueryMetric(m Metric) (string, *cmderror.CmdError) {
	response := make(chan string, 1)
	size := len(m.Addrs)
	if size > config.MaxChannelSize() {
		size = config.MaxChannelSize()
	}
	errs := make(chan *cmderror.CmdError, size)
	for _, host := range m.Addrs {
		url := "http://" + host + m.SubUri
		go httpGet(url, m.timeout, response, errs)
	}
	var retStr string
	var vecErrs []*cmderror.CmdError
	count := 0
	for err := range errs {
		if err.Code != cmderror.CODE_SUCCESS {
			vecErrs = append(vecErrs, err)
		} else {
			retStr = <-response
			vecErrs = append(vecErrs, cmderror.ErrSuccess())
			break
		}
		count++
		if count >= len(m.Addrs) {
			// all host failed
			break
		}
	}
	retErr := cmderror.MostImportantCmdError(vecErrs)
	return retStr, retErr
}

func GetMetricValue(metricRet string) (string, *cmderror.CmdError) {
	kv := cobrautil.RmWitespaceStr(metricRet)
	kvVec := strings.Split(kv, ":")
	if len(kvVec) != 2 {
		err := cmderror.ErrParseMetric()
		err.Format(metricRet)
		return "", err
	}
	kvVec[1] = strings.Replace(kvVec[1], "\"", "", -1)
	return kvVec[1], cmderror.ErrSuccess()
}

func GetKeyValueFromJsonMetric(metricRet string, key string) (string, *cmderror.CmdError) {
	var data map[string]interface{}
	if err := json.Unmarshal([]byte(metricRet), &data); err != nil {
		err := cmderror.ErrParseMetric()
		err.Format(metricRet)
		return "", err
	}
	return data[key].(string), cmderror.ErrSuccess()
}

func httpGet(url string, timeout time.Duration, response chan string, errs chan *cmderror.CmdError) {
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		interErr := cmderror.ErrHttpCreateGetRequest()
		interErr.Format(err.Error())
		errs <- interErr
	}
	// for get curl url
	req.Header.Set("User-Agent", CURL_VERSION)
	client := http.Client{
		Timeout: timeout,
	}
	resp, err := client.Do(req)
	if err != nil {
		interErr := cmderror.ErrHttpClient()
		interErr.Format(err.Error())
		errs <- interErr
	} else if resp.StatusCode != http.StatusOK {
		statusErr := cmderror.ErrHttpStatus(resp.StatusCode)
		statusErr.Format(url, resp.StatusCode)
		errs <- statusErr
	} else {
		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			interErr := cmderror.ErrHttpUnreadableResult()
			interErr.Format(url, err.Error())
			errs <- interErr
		}
		// get response
		response <- string(body)
		errs <- cmderror.ErrSuccess()
	}
}

type Rpc struct {
	Addrs         []string
	RpcTimeout    time.Duration
	RpcRetryTimes int32
	RpcFuncName   string
}

func NewRpc(addrs []string, timeout time.Duration, retryTimes int32, funcName string) *Rpc {
	return &Rpc{
		Addrs:         addrs,
		RpcTimeout:    timeout,
		RpcRetryTimes: retryTimes,
		RpcFuncName:   funcName,
	}
}

type RpcFunc interface {
	NewRpcClient(cc grpc.ClientConnInterface)
	Stub_Func(ctx context.Context) (interface{}, error)
}

func GetRpcResponse(rpc Rpc, rpcFunc RpcFunc) (interface{}, []*cmderror.CmdError) {
	size := len(rpc.Addrs)
	if size > config.MaxChannelSize() {
		size = config.MaxChannelSize()
	}
	errs := make(chan *cmderror.CmdError, size)
	response := make(chan interface{}, 1)
	for _, addr := range rpc.Addrs {
		go func(addr string) {
			ctx, cancel := context.WithTimeout(context.Background(), rpc.RpcTimeout)
			defer cancel()
			conn, err := grpc.DialContext(ctx, addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				errDial := cmderror.ErrRpcDial()
				errDial.Format(addr, err.Error())
				errs <- errDial
			}
			rpcFunc.NewRpcClient(conn)
			res, err := rpcFunc.Stub_Func(context.Background())
			if err != nil {
				// fmt.Println(err)
				errRpc := cmderror.ErrRpcCall()
				errRpc.Format(addr, rpc.RpcFuncName, err.Error())
				errs <- errRpc
			} else {
				response <- res
				errs <- cmderror.ErrSuccess()
			}
		}(addr)
	}

	var ret interface{}
	var vecErrs []*cmderror.CmdError
	count := 0
	for err := range errs {
		if err.Code != cmderror.CODE_SUCCESS {
			vecErrs = append(vecErrs, err)
		} else {
			ret = <-response
			vecErrs = append(vecErrs, cmderror.ErrSuccess())
			break
		}
		count++
		if count >= len(rpc.Addrs) {
			// all host failed
			break
		}
	}
	return ret, vecErrs
}
