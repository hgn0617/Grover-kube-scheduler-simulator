package config

import (
	"context"

	"golang.org/x/xerrors"
	apiruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/klog/v2"
	configv1 "k8s.io/kube-scheduler/config/v1"
	"k8s.io/kubernetes/pkg/scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins"
	frameworkruntime "k8s.io/kubernetes/pkg/scheduler/framework/runtime"

	"sigs.k8s.io/kube-scheduler-simulator/simulator/scheduler/plugin/quantum"
)

// outOfTreeRegistries holds custom (out-of-tree) plugins.
// It is empty by default and populated via SetOutOfTreeRegistries.
var outOfTreeRegistries = frameworkruntime.Registry{}

// RegisteredMultiPointPluginNames returns all registered multipoint plugin names.
// in-tree plugins and your original plugins listed in outOfTreeRegistries above.
func RegisteredMultiPointPluginNames() ([]string, error) {
	def, err := InTreeMultiPointPluginSet()
	if err != nil {
		return nil, xerrors.Errorf("get default multi point plugins: %w", err)
	}

	enabledPls := make([]string, 0, len(def.Enabled))
	for _, e := range def.Enabled {
		enabledPls = append(enabledPls, e.Name)
	}

	return append(enabledPls, OutOfTreeMultiPointPluginNames()...), nil
}

// InTreeMultiPointPluginSet returns default multipoint plugins.
// See also: https://github.com/kubernetes/kubernetes/blob/475f9010f5faa7bdd439944a6f5f1ec206297602/pkg/scheduler/apis/config/v1/default_plugins.go#L30https://github.com/kubernetes/kubernetes/blob/475f9010f5faa7bdd439944a6f5f1ec206297602/pkg/scheduler/apis/config/v1/default_plugins.go#L30
func InTreeMultiPointPluginSet() (configv1.PluginSet, error) {
	defaultConfig, err := DefaultSchedulerConfig()
	if err != nil || len(defaultConfig.Profiles) != 1 {
		// default Config should only have default-scheduler configuration.
		return configv1.PluginSet{}, xerrors.Errorf("get default scheduler configuration: %w", err)
	}
	return defaultConfig.Profiles[0].Plugins.MultiPoint, nil
}

func OutOfTreeMultiPointPluginNames() []string {
	registeredOutOfTreeMultiPointName := make([]string, 0, len(outOfTreeRegistries))
	for k := range outOfTreeRegistries {
		registeredOutOfTreeMultiPointName = append(registeredOutOfTreeMultiPointName, k)
	}
	return registeredOutOfTreeMultiPointName
}

func InTreeRegistries() frameworkruntime.Registry {
	return plugins.NewInTreeRegistry()
}

func OutOfTreeRegistries() frameworkruntime.Registry {
	// FORCE INJECTION: Ensure QuantumScheduler is always present when requested.
	if _, ok := outOfTreeRegistries[quantum.Name]; !ok {
		klog.InfoS("Force injecting QuantumScheduler into registry")
		outOfTreeRegistries[quantum.Name] = func(_ context.Context, args apiruntime.Object, handle framework.Handle) (framework.Plugin, error) {
			return quantum.New(args, handle)
		}
	}
	klog.InfoS("OutOfTreeRegistries accessed", "current_plugins", keysOfRegistry(outOfTreeRegistries))
	return outOfTreeRegistries
}

func SetOutOfTreeRegistries(r frameworkruntime.Registry) {
	klog.InfoS("SetOutOfTreeRegistries called", "plugins", keysOfRegistry(r))
	for k, v := range r {
		outOfTreeRegistries[k] = v
	}
}

// keysOfRegistry returns the plugin names in a registry for logging.
func keysOfRegistry(r frameworkruntime.Registry) []string {
	keys := make([]string, 0, len(r))
	for k := range r {
		keys = append(keys, k)
	}
	return keys
}
