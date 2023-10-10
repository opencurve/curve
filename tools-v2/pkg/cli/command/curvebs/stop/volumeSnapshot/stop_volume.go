package volumeSnapshot

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	cmderror "github.com/opencurve/curve/tools-v2/internal/error"
	basecmd "github.com/opencurve/curve/tools-v2/pkg/cli/command"
)

const (
	VERSION = "0.0.6"
	LIMIT   = "100"
	OFFSET  = "0"
)

type Stop struct {
	serverAddress []string
	timeout       time.Duration

	User     string
	FileName string
	UUID     string

	messages []string
}

func newStopSnapShot(serverAddress []string, timeout time.Duration, user, fileName, uuid string) *Stop {
	return &Stop{
		serverAddress: serverAddress,
		timeout:       timeout,
		User:          user,
		FileName:      fileName,
		UUID:          uuid,
	}
}

type Record struct {
	UUID string `json:"UUID"`
	User string `json:"User"`
	File string `json:"File"`

	Result string `json:"Result"`
}

func (s *Stop) queryStopBy() ([]*Record, *cmderror.CmdError) {
	records, err := s.getSnapShotListAll()
	return records, err
}

func (s *Stop) getSnapShotListAll() ([]*Record, *cmderror.CmdError) {
	var receiveRecords []*Record
	params := QueryParams{
		Action:  "GetFileSnapshotList",
		Version: VERSION,
		User:    s.User,
		UUID:    s.UUID,
		File:    s.FileName,
		Limit:   LIMIT,
		Offset:  OFFSET,
	}
	for {
		records, err := s.getSnapShotList(params)
		if err != nil || len(records) == 0 || records == nil {
			return receiveRecords, err
		}
		receiveRecords = append(receiveRecords, records...)

		params.Offset = params.Offset + params.Limit
	}
}

type QueryParams struct {
	Action  string `json:"Action"`
	Version string `json:"Version"`
	User    string `json:"User"`
	File    string `json:"File"`
	UUID    string `json:"UUID"`
	Limit   string `json:"limit"`
	Offset  string `json:"Offset"`
}

func (s *Stop) getSnapShotList(params QueryParams) ([]*Record, *cmderror.CmdError) {
	var resp struct {
		Code       string
		Message    string
		RequestId  string
		TotalCount int
		SnapShots  []*Record
	}
	err := s.query(params, &resp)
	if err != nil || resp.Code != "0" {
		return resp.SnapShots, err
	}
	return resp.SnapShots, nil
}

func (s *Stop) query(params QueryParams, data interface{}) *cmderror.CmdError {
	encodedParams := s.encodeParam(params)

	subUri := fmt.Sprintf("/SnapshotCloneService?%s", encodedParams)

	metric := basecmd.NewMetric(s.serverAddress, subUri, s.timeout)

	result, err := basecmd.QueryMetric(metric)
	if err.TypeCode() != cmderror.CODE_SUCCESS {
		return err
	}

	if err := json.Unmarshal([]byte(result), &data); err != nil {
		retErr := cmderror.ErrUnmarshalJson()
		retErr.Format(err.Error())
		return retErr
	}

	return nil
}

func (s *Stop) encodeParam(params QueryParams) string {
	paramsMap := map[string]string{}
	if params.Action == "CancelSnapshot" {
		paramsMap = map[string]string{
			"Action":  params.Action,
			"Version": params.Version,
			"User":    params.User,
			"UUID":    params.UUID,
			"File":    params.File,
		}
	} else {
		paramsMap = map[string]string{
			"Action":  params.Action,
			"Version": params.Version,
			"User":    params.User,
			"UUID":    params.UUID,
			"File":    params.File,
			"Limit":   params.Limit,
			"Offset":  params.Offset,
		}
	}

	values := strings.Builder{}
	for key, value := range paramsMap {
		if value != "" {
			values.WriteString(key)
			values.WriteString("=")
			values.WriteString(value)
			values.WriteString("&")
		}
	}
	str := values.String()
	return str[:len(str)-1]
}

func (s *Stop) stopSnapShot(uuid, user, file string) *cmderror.CmdError {
	params := QueryParams{
		Action:  "CancelSnapshot",
		Version: VERSION,
		User:    user,
		UUID:    uuid,
		File:    file,
	}

	err := s.query(params, nil)
	if err != nil {
		return err
	}
	return err
}
