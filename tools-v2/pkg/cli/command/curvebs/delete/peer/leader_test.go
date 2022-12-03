package peer

import (
	"fmt"
	"testing"

	"reflect"

	"github.com/opencurve/curve/tools-v2/proto/proto/common"
)

func ptrUint64(val uint64) *uint64 {
	return &val
}

func ptrString(val string) *string {
	return &val
}

func TestParseConfiguration(t *testing.T) {
	testCases := []struct {
		desc      string
		input     []string
		want      *Configuration
		wantError error
	}{
		{
			desc:      "empty group",
			wantError: fmt.Errorf("invalid peer args, err: empty group configuration"),
		},
		{
			desc:      "error format1",
			input:     []string{"1.1.1.1:90"},
			wantError: fmt.Errorf("invalid peer args, err: error format for the peer info 1.1.1.1:90"),
		},
		{
			desc:      "error format2",
			input:     []string{"1.1.1.1:90:0:1"},
			wantError: fmt.Errorf("invalid peer args, err: error format for the peer info 1.1.1.1:90:0:1"),
		},
		{
			desc:      "error format3",
			input:     []string{"1.1.1.1:90:ds"},
			wantError: fmt.Errorf("invalid peer args, err: error format for the peer id ds"),
		},
		{
			desc:  "success",
			input: []string{"1.1.1.1:90:0", "1.1.1.1:91:1", "1.1.1.1:92:2"},
			want: &Configuration{
				Peers: []*common.Peer{
					{
						Id:      ptrUint64(0),
						Address: ptrString("1.1.1.1:90"),
					},
					{
						Id:      ptrUint64(1),
						Address: ptrString("1.1.1.1:91"),
					},
					{
						Id:      ptrUint64(2),
						Address: ptrString("1.1.1.1:92"),
					},
				},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			got, err := ParseConfiguration(tc.input)
			if !cmpError(err.ToError(), tc.wantError) {
				t.Errorf("want error %s is not same with got error %s", tc.wantError, err.ToError())
			}
			if !reflect.DeepEqual(got, tc.want) {
				t.Errorf("diff: want %v as not want  %s", tc.want, got)
			}
		})
	}
}

// cmpError
// true indicates the errors are same.
func cmpError(e1, e2 error) bool {
	return (e1 == nil && e2 == nil) ||
		(e1 != nil && e2 != nil && e1.Error() == e2.Error())
}
