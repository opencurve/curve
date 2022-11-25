// package topo

// import (
// 	basecmd "github.com/opencurve/curve/tools-v2/pkg/cli/command"
// 	"github.com/opencurve/curve/tools-v2/pkg/config"
// 	"github.com/opencurve/curve/tools-v2/pkg/output"
// 	"github.com/spf13/cobra"
// )

// const (
// 	topoExample = `$ curve bs list topology`
// )

// type TopoCmd struct {
// 	basecmd.FinalCurveCmd
// }

// func NewTopoCmd() *cobra.Command {
// 	return NewListTopoCmd().Cmd
// }

// func NewListTopoCmd() *TopoCmd {
// 	ltCmd := &TopoCmd{
// 		FinalCurveCmd: basecmd.FinalCurveCmd{
// 			Use:     "physicalpool",
// 			Short:   "cluster topology without logicalpool in curvebs",
// 			Example: topoExample,
// 		},
// 	}

// 	basecmd.NewFinalCurveCli(&ltCmd.FinalCurveCmd, ltCmd)
// 	return ltCmd
// }

// func (tCmd *TopoCmd) AddFlags() {
// 	config.AddBsMdsFlagOption(tCmd.Cmd)
// 	config.AddRpcRetryTimesFlag(tCmd.Cmd)
// 	config.AddRpcTimeoutFlag(tCmd.Cmd)
// }

// func (tCmd *TopoCmd) Init(cmd *cobra.Command, args []string) error {
// 	return nil
// }

// func (tCmd *TopoCmd) RunCommand(cmd *cobra.Command, args []string) error {
// 	return nil
// }

// func (tCmd *TopoCmd) Print(cmd *cobra.Command, args []string) error {
// 	return output.FinalCmdOutput(&tCmd.FinalCurveCmd, tCmd)
// }

// func (tCmd *TopoCmd) ResultPlainOutput() error {
// 	return output.FinalCmdOutputPlain(&tCmd.FinalCurveCmd)
// }
