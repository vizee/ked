package ked

import (
	"context"
	"time"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
)

type DeploymentObject struct {
	Metadata struct {
		Generation int64 `json:"generation"`
	} `json:"metadata"`
	Spec struct {
		Replicas int64 `json:"replicas"`
		Paused   bool  `json:"paused"`
	} `json:"spec"`
	Status struct {
		ObservedGeneration int64 `json:"observedGeneration"`
		Replicas           int64 `json:"replicas"`
		UpdatedReplicas    int64 `json:"updatedReplicas"`
		ReadyReplicas      int64 `json:"readyReplicas"`
	} `json:"status"`
}

func convertObjectToType[T any](obj *unstructured.Unstructured) (*T, error) {
	var typed T
	err := runtime.DefaultUnstructuredConverter.FromUnstructured(obj.Object, &typed)
	if err != nil {
		return nil, err
	}
	return &typed, nil
}

const (
	StatePending = iota
	StateDeploying
	StateReplicasUpdated
	StateReplicasReady
	StateInterrupted
)

type Event struct {
	State      int
	Ns         string
	Name       string
	Deployment *DeploymentObject
	Err        error
	Done       bool
}

type Watcher struct {
	events        chan Event
	deployTimeout time.Duration
	checkInterval time.Duration
	strict        bool
	recvEvent     int
}

func (w *Watcher) postEvent(ctx context.Context, ev Event) {
	select {
	case w.events <- ev:
	case <-ctx.Done():
	}
}

func (w *Watcher) watchDeploymentStatus(op *Operator, deployment *unstructured.Unstructured) {
	ns := deployment.GetNamespace()
	name := deployment.GetName()
	watchedGeneration := deployment.GetGeneration()
	deployState := StatePending

	var (
		lastErr        error
		lastDeployment *DeploymentObject
	)
	ctx, cancel := context.WithTimeout(context.Background(), w.deployTimeout)
	defer cancel()
	for {
		obj, err := op.kc.Get(ctx, deploymentGVK, ns, name)
		if err != nil {
			lastErr = err
			break
		}

		deployment, err := convertObjectToType[DeploymentObject](obj)
		if err != nil {
			lastErr = err
			break
		}

		if deployment.Metadata.Generation != watchedGeneration {
			deployState = StateInterrupted
			break
		}

		if deployment.Status.ObservedGeneration != watchedGeneration {
			if w.recvEvent > 1 {
				w.postEvent(ctx, Event{
					State:      deployState,
					Ns:         ns,
					Name:       name,
					Deployment: deployment,
				})
			}
		} else if deployment.Spec.Paused {
			deployState = StateInterrupted
		} else {
			switch deployState {
			case StatePending:
				if deployment.Status.UpdatedReplicas == 0 {
					break
				}
				deployState = StateDeploying

				if w.recvEvent > 0 {
					w.postEvent(ctx, Event{
						State:      StateDeploying,
						Ns:         ns,
						Name:       name,
						Deployment: deployment,
					})
				}

				fallthrough
			case StateDeploying:
				if deployment.Status.UpdatedReplicas < deployment.Spec.Replicas {
					break
				}
				deployState = StateReplicasUpdated

				if w.recvEvent > 1 {
					w.postEvent(ctx, Event{
						State:      deployState,
						Ns:         ns,
						Name:       name,
						Deployment: deployment,
					})
				}

				fallthrough
			case StateReplicasUpdated:
				if deployment.Status.ReadyReplicas == deployment.Spec.Replicas {
					if !w.strict || deployment.Status.Replicas == deployment.Spec.Replicas {
						deployState = StateReplicasReady
						lastDeployment = deployment
					}
				}
			}
		}

		if deployState >= StateReplicasReady {
			break
		}
		time.Sleep(w.checkInterval)
	}

	w.postEvent(ctx, Event{
		State:      deployState,
		Ns:         ns,
		Name:       name,
		Deployment: lastDeployment,
		Err:        lastErr,
		Done:       true,
	})
}

func (w *Watcher) Events() <-chan Event {
	return w.events
}

func NewWatcher(deployTimeout time.Duration, checkInterval time.Duration, strict bool, recvEvent int) *Watcher {
	return &Watcher{
		events:        make(chan Event, 16),
		deployTimeout: deployTimeout,
		checkInterval: checkInterval,
		strict:        strict,
		recvEvent:     recvEvent,
	}
}
