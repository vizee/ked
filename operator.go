package ked

import (
	"context"
	"fmt"
	"time"

	"github.com/vizee/ked/kube"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

var deploymentGVK = schema.GroupVersionKind{Group: "apps", Version: "v1", Kind: "Deployment"}

type AppTemplate interface {
	GetNs() string
	GetDeploymentName() string
	GenerateObjects() ([]*unstructured.Unstructured, error)
}

type Operator struct {
	prefix string
	kc     *kube.Client
}

func (op *Operator) redeployObject(ctx context.Context, obj *unstructured.Unstructured) (*unstructured.Unstructured, error) {
	var redeployObj unstructured.Unstructured
	redeployObj.SetAPIVersion(obj.GetAPIVersion())
	redeployObj.SetKind(obj.GetKind())
	redeployObj.SetNamespace(obj.GetNamespace())
	redeployObj.SetName(obj.GetName())
	deployerName := fmt.Sprintf("%s-deployer", op.prefix)

	err := unstructured.SetNestedField(redeployObj.Object, time.Now().Format(time.RFC3339),
		"spec",
		"template",
		"metadata",
		"annotations",
		deployerName+"/redeployAt")
	if err != nil {
		return nil, err
	}

	return op.kc.SSA(ctx, deployerName, &redeployObj)
}

func (op *Operator) RedeployApp(ctx context.Context, app AppTemplate, watcher *Watcher) error {
	deployment, err := op.kc.Get(ctx, deploymentGVK, app.GetNs(), app.GetDeploymentName())
	if err != nil {
		return err
	}
	last, err := op.redeployObject(ctx, deployment)
	if err != nil {
		return err
	}
	if watcher != nil {
		go watcher.watchDeploymentStatus(op, last)
	}
	return nil
}

func (op *Operator) RedeployAll(ctx context.Context, ns string, should func(*unstructured.Unstructured) bool, watcher *Watcher) error {
	list, err := op.kc.ListByLabel(ctx, deploymentGVK, ns, "")
	if err != nil {
		return err
	}
	for _, deployment := range list.Items {
		if should(&deployment) {
			last, err := op.redeployObject(ctx, &deployment)
			if err != nil {
				return err
			}
			if watcher != nil {
				go watcher.watchDeploymentStatus(op, last)
			}
		}
	}
	return nil
}

func (op *Operator) DeployApp(ctx context.Context, app AppTemplate, replace bool, watcher *Watcher) error {
	ns := app.GetNs()
	deploymentName := app.GetDeploymentName()
	objects, err := app.GenerateObjects()
	if err != nil {
		return err
	}

	appObjectIdx := -1
	for i, obj := range objects {
		if obj.GetKind() == deploymentGVK.Kind && obj.GetNamespace() == ns && obj.GetName() == deploymentName {
			appObjectIdx = i
			break
		}
	}
	if appObjectIdx == -1 {
		return fmt.Errorf("App deployment not found")
	}

	managerName := fmt.Sprintf("%s-manager", op.prefix)

	var appObject *unstructured.Unstructured
	for i, obj := range objects {
		var last *unstructured.Unstructured
		if replace {
			last, err = op.kc.Replace(ctx, managerName, obj.GroupVersionKind(), obj.GetNamespace(), obj.GetName(), func(_ *unstructured.Unstructured) (*unstructured.Unstructured, error) {
				return obj, nil
			})
			if err != nil && !errors.IsNotFound(err) {
				return err
			}
		}
		if last == nil {
			last, err = op.kc.Create(ctx, managerName, obj)
			if err != nil {
				return err
			}
		}
		if i == appObjectIdx {
			appObject = last
		}
	}

	if appObject != nil && watcher != nil {
		go watcher.watchDeploymentStatus(op, appObject)
	}

	return nil
}

func NewOperator(prefix string, kc *kube.Client) *Operator {
	return &Operator{
		prefix: prefix,
		kc:     kc,
	}
}
