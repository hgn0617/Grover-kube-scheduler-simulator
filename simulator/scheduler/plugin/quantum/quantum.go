package quantum

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/scheduler/framework"
)

const (
	Name              = "QuantumScheduler"
	quantumServiceURL = "http://quantum-service:8000"
)

// QuantumScheduler uses quantum computing to optimize pod placement
type QuantumScheduler struct {
	handle framework.Handle
	client *http.Client
	cache  *assignmentCache
}

// assignmentCache caches quantum assignments
type assignmentCache struct {
	mu          sync.RWMutex
	assignments map[string]int // pod name -> node index
	timestamp   time.Time
}

func (c *assignmentCache) get(podName string) (int, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	// Cache expires after 30 seconds
	if time.Since(c.timestamp) > 30*time.Second {
		return 0, false
	}

	nodeIdx, ok := c.assignments[podName]
	return nodeIdx, ok
}

func (c *assignmentCache) set(assignments map[string]int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.assignments = assignments
	c.timestamp = time.Now()
}

var _ framework.ScorePlugin = &QuantumScheduler{}

func (qs *QuantumScheduler) Name() string {
	return Name
}

func New(_ runtime.Object, h framework.Handle) (framework.Plugin, error) {
	klog.InfoS("QuantumScheduler plugin initializing", "serviceURL", quantumServiceURL)
	return &QuantumScheduler{
		handle: h,
		client: &http.Client{Timeout: 180 * time.Second},
		cache:  &assignmentCache{assignments: make(map[string]int)},
	}, nil
}

// PodAntiAffinity represents pod anti-affinity rules
type PodAntiAffinity struct {
	PodName       string   `json:"pod_name"`
	ConflictsWith []string `json:"conflicts_with"`
}

// ScheduleRequest is sent to quantum service
type ScheduleRequest struct {
	Pods        []PodAntiAffinity `json:"pods"`
	NumNodes    int               `json:"num_nodes"`
	MaxAttempts int               `json:"max_attempts"`
}

// ScheduleResponse from quantum service
type ScheduleResponse struct {
	Success          bool           `json:"success"`
	Assignments      map[string]int `json:"assignments"`
	Attempts         int            `json:"attempts"`
	Message          string         `json:"message"`
	GroverIterations int            `json:"grover_iterations"`
	Error            string         `json:"error,omitempty"`
}

// callQuantumService calls the quantum scheduler service
func (qs *QuantumScheduler) callQuantumService(ctx context.Context, pod *v1.Pod) (*ScheduleResponse, error) {
	klog.InfoS("QuantumScheduler.callQuantumService start", "pod", pod.Name)
	// Check if pod has anti-affinity
	if pod.Spec.Affinity == nil || pod.Spec.Affinity.PodAntiAffinity == nil {
		klog.V(4).InfoS("QuantumScheduler: no podAntiAffinity, skipping quantum", "pod", pod.Name)
		return nil, nil
	}

	// Extract conflicts
	conflicts := []string{}
	antiAffinity := pod.Spec.Affinity.PodAntiAffinity
	if antiAffinity.RequiredDuringSchedulingIgnoredDuringExecution != nil {
		for _, term := range antiAffinity.RequiredDuringSchedulingIgnoredDuringExecution {
			if term.LabelSelector != nil && term.LabelSelector.MatchExpressions != nil {
				for _, expr := range term.LabelSelector.MatchExpressions {
					if expr.Operator == "In" {
						conflicts = append(conflicts, expr.Values...)
					}
				}
			}
		}
	}

	if len(conflicts) == 0 {
		klog.V(4).InfoS("QuantumScheduler: no conflicts extracted, skipping quantum", "pod", pod.Name)
		return nil, nil
	}

	// Get node count
	nodeInfos, err := qs.handle.SnapshotSharedLister().NodeInfos().List()
	if err != nil {
		return nil, err
	}

	// Build request
	pods := []PodAntiAffinity{{
		PodName:       pod.Name,
		ConflictsWith: conflicts,
	}}

	request := ScheduleRequest{
		Pods:        pods,
		NumNodes:    len(nodeInfos),
		MaxAttempts: 20,
	}

	jsonData, err := json.Marshal(request)
	if err != nil {
		return nil, err
	}

	klog.InfoS("QuantumScheduler: calling quantum-service /schedule",
		"pod", pod.Name,
		"conflicts", len(conflicts),
		"nodes", len(nodeInfos))

	resp, err := qs.client.Post(
		quantumServiceURL+"/schedule",
		"application/json",
		bytes.NewBuffer(jsonData),
	)
	if err != nil {
		klog.ErrorS(err, "Failed to call quantum service")
		return nil, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	var response ScheduleResponse
	if err := json.Unmarshal(body, &response); err != nil {
		return nil, err
	}

	klog.InfoS("QuantumScheduler: quantum-service response",
		"success", response.Success,
		"attempts", response.Attempts,
		"groverIterations", response.GroverIterations,
		"assignments", response.Assignments)

	if response.Success {
		qs.cache.set(response.Assignments)
	}

	return &response, nil
}

// Score ranks nodes for the pod
func (qs *QuantumScheduler) Score(ctx context.Context, state *framework.CycleState, pod *v1.Pod, nodeName string) (int64, *framework.Status) {
	klog.V(4).InfoS("QuantumScheduler.Score called", "pod", pod.Name, "node", nodeName)

	// Priority 1: Check if Operator has pre-computed assignment via annotation
	if pod.Annotations != nil {
		if recommendedNode, ok := pod.Annotations["quantum-scheduler.io/recommended-node"]; ok {
			if nodeName == recommendedNode {
				klog.InfoS("QuantumScheduler: using Operator pre-computed assignment",
					"pod", pod.Name,
					"recommendedNode", recommendedNode,
					"score", 100)
				return 100, framework.NewStatus(framework.Success)
			}
			// Node doesn't match pre-computed assignment, return very low score
			klog.V(4).InfoS("QuantumScheduler: node doesn't match pre-computed assignment",
				"pod", pod.Name,
				"node", nodeName,
				"recommended", recommendedNode)
			return 0, framework.NewStatus(framework.Success)
		}
	}

	// Priority 2: Check cache from previous quantum service calls
	if nodeIdx, ok := qs.cache.get(pod.Name); ok {
		expectedNodeName := fmt.Sprintf("node-%d", nodeIdx)
		if nodeName == expectedNodeName {
			klog.V(4).InfoS("Quantum boost from cache", "pod", pod.Name, "node", nodeName)
			return 100, framework.NewStatus(framework.Success)
		}
		return 0, framework.NewStatus(framework.Success)
	}

	// Priority 3: Call quantum service for real-time computation (fallback)
	response, err := qs.callQuantumService(ctx, pod)
	if err != nil {
		klog.V(4).InfoS("Quantum service error, using default", "error", err)
		return 0, framework.NewStatus(framework.Success)
	}

	if response == nil || !response.Success {
		return 0, framework.NewStatus(framework.Success)
	}

	// Check if this node is preferred
	if nodeIdx, ok := response.Assignments[pod.Name]; ok {
		expectedNodeName := fmt.Sprintf("node-%d", nodeIdx)
		if nodeName == expectedNodeName {
			klog.InfoS("Quantum recommendation",
				"pod", pod.Name,
				"preferredNode", nodeName,
				"attempts", response.Attempts)
			return 100, framework.NewStatus(framework.Success)
		}
	}

	return 0, framework.NewStatus(framework.Success)
}

// ScoreExtensions returns nil
func (qs *QuantumScheduler) ScoreExtensions() framework.ScoreExtensions {
	return nil
}
