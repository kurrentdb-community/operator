package controllers

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	kurrentv1 "github.com/kurrentdb-community/operator/api/v1"
)

type gossipMember struct {
	InstanceID             string `json:"instanceId"`
	State                  string `json:"state"`
	IsAlive                bool   `json:"isAlive"`
	InternalHttpEndPointIP string `json:"internalHttpEndPointIp"`
	HTTPEndPointIP         string `json:"httpEndPointIp"`
	HTTPEndPointPort       int32  `json:"httpEndPointPort"`
}

type gossipResponse struct {
	Members  []gossipMember `json:"members"`
	ServerIP string         `json:"serverIp"`
}

func (r *KurrentClusterReconciler) reconcileLeader(ctx context.Context, cluster *kurrentv1.KurrentCluster) error {
	logger := log.FromContext(ctx)

	podList := &corev1.PodList{}
	listOpts := []client.ListOption{
		client.InNamespace(cluster.Namespace),
		client.MatchingLabels{
			"app.kubernetes.io/name":     "kurrentdb",
			"app.kubernetes.io/instance": cluster.Name,
		},
	}
	if err := r.List(ctx, podList, listOpts...); err != nil {
		return err
	}

	// Build quick lookups.
	ipToPod := make(map[string]*corev1.Pod, len(podList.Items))
	var currentlyLeaderPods []*corev1.Pod
	for i := range podList.Items {
		p := &podList.Items[i]
		if p.Status.PodIP != "" {
			ipToPod[p.Status.PodIP] = p
		}
		if p.Labels != nil && p.Labels["community.kurrent.io/role"] == "leader" {
			currentlyLeaderPods = append(currentlyLeaderPods, p)
		}
	}

	leaderIP, err := r.discoverLeaderIP(ctx, cluster, podList.Items)
	if err != nil {
		// IMPORTANT: do not mutate labels if discovery fails.
		return err
	}

	leaderPod := ipToPod[leaderIP]
	if leaderPod == nil {
		// We found "a leader", but can't map it to a PodIP we know about.
		// Don't churn labels; requeue sooner by returning an error.
		return fmt.Errorf("leader ip %q not found in pod list", leaderIP)
	}

	// 1) Ensure leader pod is labeled
	if leaderPod.Labels == nil || leaderPod.Labels["community.kurrent.io/role"] != "leader" {
		old := leaderPod.DeepCopy()
		patch := leaderPod.DeepCopy()
		if patch.Labels == nil {
			patch.Labels = make(map[string]string)
		}
		patch.Labels["community.kurrent.io/role"] = "leader"

		if err := r.Patch(ctx, patch, client.MergeFrom(old)); err != nil {
			return err
		}
		logger.Info("Leader pod identified and labeled", "pod", leaderPod.Name, "ip", leaderIP)
	}

	// 2) Remove leader label from any other pods currently marked leader
	for _, p := range currentlyLeaderPods {
		if p.Name == leaderPod.Name {
			continue
		}
		old := p.DeepCopy()
		patch := p.DeepCopy()
		if patch.Labels != nil {
			delete(patch.Labels, "community.kurrent.io/role")
		}
		if err := r.Patch(ctx, patch, client.MergeFrom(old)); err != nil {
			logger.Error(err, "Failed to clear stale leader label", "pod", p.Name)
			return err
		}
	}

	return nil
}

func (r *KurrentClusterReconciler) discoverLeaderIP(ctx context.Context, cluster *kurrentv1.KurrentCluster, pods []corev1.Pod) (string, error) {
	logger := log.FromContext(ctx)

	var leaderIP string
	httpClient := &http.Client{
		Timeout: 5 * time.Second,
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
		},
	}

	for _, pod := range pods {
		if pod.Status.Phase != corev1.PodRunning || pod.Status.PodIP == "" {
			continue
		}

		isReady := false
		for _, cond := range pod.Status.Conditions {
			if cond.Type == corev1.PodReady && cond.Status == corev1.ConditionTrue {
				isReady = true
				break
			}
		}
		if !isReady {
			continue
		}

		scheme := "http"
		if cluster.Spec.TLS.Enabled {
			scheme = "https"
		}

		url := fmt.Sprintf("%s://%s:%d/gossip", scheme, pod.Status.PodIP, cluster.Spec.Network.GossipPort)
		resp, err := httpClient.Get(url)
		if err != nil {
			logger.V(1).Info("Failed to get gossip from pod", "pod", pod.Name, "error", err)
			continue
		}

		if resp.StatusCode != http.StatusOK {
			_ = resp.Body.Close()
			continue
		}

		body, err := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if err != nil {
			continue
		}

		var gossip gossipResponse
		if err := json.Unmarshal(body, &gossip); err != nil {
			continue
		}

		for _, member := range gossip.Members {
			if member.State == "Leader" && member.IsAlive {
				leaderIP = member.InternalHttpEndPointIP
				if leaderIP == "" {
					leaderIP = member.HTTPEndPointIP
				}
				break
			}
		}

		if leaderIP != "" {
			break
		}
	}

	// If we couldn't determine the leader, return an error (do NOT mutate labels).
	if leaderIP == "" {
		return "", fmt.Errorf("unable to determine leader from gossip")
	}
	return leaderIP, nil
}
