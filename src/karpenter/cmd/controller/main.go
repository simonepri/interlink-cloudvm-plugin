// Entry point of the CloudVM Karpenter controller: Karpenter core v1.15 controllers driving the CloudVM provider.
// Karpenter settings come from its usual flags and environment; CLOUDVM_* variables configure the provider.
package main

import (
	"fmt"
	"os"

	"github.com/samber/lo"
	"sigs.k8s.io/karpenter/pkg/cloudprovider/overlay"
	"sigs.k8s.io/karpenter/pkg/controllers"
	"sigs.k8s.io/karpenter/pkg/controllers/node/health"
	"sigs.k8s.io/karpenter/pkg/controllers/state"
	"sigs.k8s.io/karpenter/pkg/operator"

	_ "github.com/simonepri/interlink-cloudvm-plugin/karpenter/apis/v1alpha1" // registers the NodeClass kind
	"github.com/simonepri/interlink-cloudvm-plugin/karpenter/cloudprovider"
	"github.com/simonepri/interlink-cloudvm-plugin/karpenter/pluginclient"
)

func main() {
	pluginURL := os.Getenv("CLOUDVM_PLUGIN_URL")
	if pluginURL == "" {
		fmt.Fprintln(os.Stderr, "CLOUDVM_PLUGIN_URL is required")
		os.Exit(2)
	}
	cfg, err := cloudprovider.LoadConfig(os.Getenv("CLOUDVM_CONFIG_PATH"))
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	api, err := pluginclient.New(pluginURL, os.Getenv("CLOUDVM_PLUGIN_TOKEN_FILE"))
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}

	ctx, op := operator.NewOperator()
	undecorated := cloudprovider.New(op.GetClient(), api, cfg, op.Clock)
	cloudProvider := overlay.Decorate(undecorated, op.GetClient(), op.InstanceTypeStore)
	repairPolicyMatcher := lo.Must(health.NewRepairPolicyMatcher(ctx, cloudProvider))
	clusterState := state.NewCluster(op.Clock, op.GetClient(), cloudProvider, state.WithRepairPolicyMatcher(repairPolicyMatcher))
	op.
		WithControllers(ctx, controllers.NewControllers(
			ctx,
			op.Manager,
			op.Clock,
			op.GetClient(),
			op.EventRecorder,
			cloudProvider,
			undecorated,
			clusterState,
			op.InstanceTypeStore,
			op.PredictionStore,
		)...).
		Start(ctx)
}
