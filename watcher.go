package ked

import (
	"context"
	"time"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
)

type deploymentObject struct {
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
	State int
	Ns    string
	Name  string
	Err   error
	Done  bool
}

type Watcher struct {
	ev            chan Event
	deployTimeout time.Duration
	checkInterval time.Duration
	recvEvent     int
}

func (w *Watcher) watchDeploymentStatus(op *Operator, deployment *unstructured.Unstructured) {
	ns := deployment.GetNamespace()
	name := deployment.GetName()
	watchedGeneration := deployment.GetGeneration()
	deployState := StatePending

	var lastErr error
	ctx, cancel := context.WithTimeout(context.Background(), w.deployTimeout)
	defer cancel()
	for {
		obj, err := op.kc.Get(ctx, deploymentGVK, ns, name)
		if err != nil {
			lastErr = err
			break
		}

		deployment, err := convertObjectToType[deploymentObject](obj)
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
				w.ev <- Event{
					State: deployState,
					Ns:    ns,
					Name:  name,
				}
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
					w.ev <- Event{
						State: StateDeploying,
						Ns:    ns,
						Name:  name,
					}
				}

				fallthrough
			case StateDeploying:
				if deployment.Status.UpdatedReplicas < deployment.Spec.Replicas {
					break
				}
				deployState = StateReplicasUpdated

				if w.recvEvent > 1 {
					w.ev <- Event{
						State: deployState,
						Ns:    ns,
						Name:  name,
					}
				}

				fallthrough
			case StateReplicasUpdated:
				if deployment.Status.ReadyReplicas == deployment.Spec.Replicas {
					deployState = StateReplicasReady
				}
			}
		}

		if deployState >= StateReplicasReady {
			break
		}
		time.Sleep(w.checkInterval)
	}

	w.ev <- Event{
		State: deployState,
		Ns:    ns,
		Name:  name,
		Err:   lastErr,
		Done:  true,
	}
}

func NewWatcher(deployTimeout time.Duration, checkInterval time.Duration, recvEvent int) *Watcher {
	return &Watcher{
		ev:            make(chan Event, 16),
		deployTimeout: deployTimeout,
		checkInterval: checkInterval,
		recvEvent:     recvEvent,
	}
}
