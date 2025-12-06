package main

import (
	"context"
	"fmt"
	"os"

	apiruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/component-base/cli"
	_ "k8s.io/component-base/logs/json/register" // for JSON log format registration
	_ "k8s.io/component-base/metrics/prometheus/clientgo"
	_ "k8s.io/component-base/metrics/prometheus/version" // for version metric registration
	"k8s.io/klog"
	"k8s.io/kubernetes/pkg/scheduler/framework"

	"sigs.k8s.io/kube-scheduler-simulator/simulator/pkg/debuggablescheduler"
	"sigs.k8s.io/kube-scheduler-simulator/simulator/scheduler/plugin/quantum"
)

// quantumFactory adapts quantum.New to the scheduler's PluginFactory signature.
func quantumFactory(_ context.Context, cfg apiruntime.Object, h framework.Handle) (framework.Plugin, error) {
	return quantum.New(cfg, h)
}

func main() {
	fmt.Println("[Grover-main] Custom scheduler main with Quantum plugin starting")
	klog.Info("[Grover] Custom scheduler main with Quantum plugin starting")
	command, cancelFn, err := debuggablescheduler.NewSchedulerCommand(
		debuggablescheduler.WithPlugin(quantum.Name, quantumFactory),
	)
	if err != nil {
		klog.Info(fmt.Sprintf("failed to build the debuggablescheduler command: %+v", err))
		os.Exit(1)
	}
	code := cli.Run(command)

	cancelFn()
	os.Exit(code)
}
