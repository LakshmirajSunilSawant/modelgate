// Command kubectl-modelgate is a kubectl plugin for manual rollout control:
//
//	kubectl modelgate status  <rollout> [-n ns] [-w]
//	kubectl modelgate pause   <rollout> [-n ns]
//	kubectl modelgate resume  <rollout> [-n ns]
//	kubectl modelgate abort   <rollout> [-n ns]
//	kubectl modelgate promote <rollout> --force [-n ns]
//
// Every action records the caller's Kubernetes identity, which the
// controller writes into the rollout's events.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strings"
	"text/tabwriter"
	"time"

	authv1 "k8s.io/api/authentication/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"sigs.k8s.io/controller-runtime/pkg/client"

	mgv1 "github.com/modelgate/modelgate/api/v1alpha1"
)

const usage = `usage: kubectl modelgate <status|pause|resume|abort|promote> <rollout> [-n namespace] [--force] [-w]`

func main() {
	if len(os.Args) < 3 {
		fail(usage)
	}
	cmd, name := os.Args[1], os.Args[2]
	fs := flag.NewFlagSet("modelgate", flag.ExitOnError)
	ns := fs.String("n", "", "namespace (defaults to the kubeconfig context's)")
	force := fs.Bool("force", false, "required for promote: skip remaining analysis")
	watch := fs.Bool("w", false, "status: refresh every 2 seconds until interrupted")
	_ = fs.Parse(os.Args[3:])

	loader := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(clientcmd.NewDefaultClientConfigLoadingRules(), &clientcmd.ConfigOverrides{})
	cfg, err := loader.ClientConfig()
	check(err)
	if *ns == "" {
		*ns, _, err = loader.Namespace()
		check(err)
	}
	scheme := runtime.NewScheme()
	check(mgv1.AddToScheme(scheme))
	c, err := client.New(cfg, client.Options{Scheme: scheme})
	check(err)

	ctx := context.Background()
	mr := &mgv1.ModelRollout{}
	check(c.Get(ctx, types.NamespacedName{Namespace: *ns, Name: name}, mr))

	var patch map[string]any
	switch cmd {
	case "status":
		if !*watch {
			printStatus(mr)
			return
		}
		for {
			fmt.Print("\033[H\033[2J") // clear the terminal
			fmt.Printf("%s  (refreshing every 2s, Ctrl-C to stop)\n\n", time.Now().Format("15:04:05"))
			printStatus(mr)
			time.Sleep(2 * time.Second)
			fresh := &mgv1.ModelRollout{}
			if err := c.Get(ctx, types.NamespacedName{Namespace: *ns, Name: name}, fresh); err == nil {
				mr = fresh
			}
		}
	case "pause", "resume":
		patch = map[string]any{
			"metadata": map[string]any{"annotations": map[string]string{mgv1.ActionByAnnotation: whoami(ctx, cfg)}},
			"spec":     map[string]any{"paused": cmd == "pause"},
		}
	case "abort":
		patch = actionPatch("abort", whoami(ctx, cfg))
	case "promote":
		if !*force {
			fail("promote skips the quality gate; pass --force to confirm")
		}
		patch = actionPatch("promote", whoami(ctx, cfg))
	default:
		fail(usage)
	}
	body, _ := json.Marshal(patch)
	check(c.Patch(ctx, mr, client.RawPatch(types.MergePatchType, body)))
	fmt.Printf("modelrollout %s/%s: %s requested\n", *ns, name, cmd)
}

func actionPatch(action, by string) map[string]any {
	return map[string]any{"metadata": map[string]any{"annotations": map[string]string{
		mgv1.ActionAnnotation: action, mgv1.ActionByAnnotation: by,
	}}}
}

// whoami asks the API server who we are, so the audit trail records the
// authenticated identity rather than a local username.
func whoami(ctx context.Context, cfg *rest.Config) string {
	cs, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return "unknown"
	}
	r, err := cs.AuthenticationV1().SelfSubjectReviews().Create(ctx, &authv1.SelfSubjectReview{}, metav1.CreateOptions{})
	if err != nil || r.Status.UserInfo.Username == "" {
		return "unknown"
	}
	return r.Status.UserInfo.Username
}

func printStatus(mr *mgv1.ModelRollout) {
	st := mr.Status
	w := tabwriter.NewWriter(os.Stdout, 0, 4, 2, ' ', 0)
	fmt.Fprintf(w, "Rollout:\t%s/%s\n", mr.Namespace, mr.Name)
	fmt.Fprintf(w, "Phase:\t%s\n", st.Phase)
	fmt.Fprintf(w, "Message:\t%s\n", st.Message)
	fmt.Fprintf(w, "Stable:\t%s\n", st.StableRevision)
	if st.CanaryRevision != "" {
		stage := fmt.Sprintf("step %d/%d at %d%%", st.CurrentStep+1, len(mr.Spec.Strategy.Steps), st.CanaryWeight)
		if st.CurrentStep == mgv1.ShadowStepIndex {
			stage = "shadow"
		}
		fmt.Fprintf(w, "Canary:\t%s (%s)\n", st.CanaryRevision, stage)
	}
	if st.AbortedRevision != "" {
		fmt.Fprintf(w, "Last rolled back:\t%s\n", st.AbortedRevision)
	}
	w.Flush()
	if v := st.LastVerdict; v != nil {
		fmt.Printf("\nLast verdict: %s at %s (stable %d, canary %d, paired %d samples)\n  %s\n\n",
			v.Decision, v.Time.Format("15:04:05"), v.StableSamples, v.CanarySamples, v.PairedSamples, v.Reason)
		w = tabwriter.NewWriter(os.Stdout, 0, 4, 2, ' ', 0)
		fmt.Fprintln(w, "CHECK\tSTATUS\tVALUE\tTHRESHOLD")
		for _, c := range v.Checks {
			fmt.Fprintf(w, "%s\t%s\t%s\t%s\n", c.Name, strings.ToUpper(c.Status), c.Value, c.Threshold)
		}
		w.Flush()
		if len(v.Disagreements) > 0 {
			fmt.Println("\nSample disagreements (stable -> canary):")
			for _, d := range v.Disagreements {
				fmt.Printf("  %s: %s%s -> %s%s\n", d.RequestID, d.Stable.Label, score(d.Stable.Score), d.Canary.Label, score(d.Canary.Score))
			}
		}
	}
}

func score(s string) string {
	if s == "" {
		return ""
	}
	return " (" + s + ")"
}

func check(err error) {
	if err != nil {
		fail(err.Error())
	}
}

func fail(msg string) {
	fmt.Fprintln(os.Stderr, msg)
	os.Exit(1)
}
