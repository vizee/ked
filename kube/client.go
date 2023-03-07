package kube

import (
	"context"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/discovery/cached/memory"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/restmapper"
)

type Client struct {
	di     dynamic.Interface
	mapper *restmapper.DeferredDiscoveryRESTMapper
}

func (c *Client) getResourceInterface(gvk schema.GroupVersionKind, ns string) (dynamic.ResourceInterface, error) {
	gvr, err := c.mapper.RESTMapping(gvk.GroupKind(), gvk.Version)
	if err != nil {
		return nil, err
	}
	var ri dynamic.ResourceInterface
	if gvr.Scope.Name() == meta.RESTScopeNameNamespace {
		ri = c.di.Resource(gvr.Resource).Namespace(ns)
	} else {
		ri = c.di.Resource(gvr.Resource)
	}
	return ri, nil
}

func (c *Client) Get(ctx context.Context, gvk schema.GroupVersionKind, ns string, name string) (*unstructured.Unstructured, error) {
	ri, err := c.getResourceInterface(gvk, ns)
	if err != nil {
		return nil, err
	}
	return ri.Get(ctx, name, metav1.GetOptions{})
}

func (c *Client) ListByLabel(ctx context.Context, gvk schema.GroupVersionKind, ns string, labelSelector string) (*unstructured.UnstructuredList, error) {
	ri, err := c.getResourceInterface(gvk, ns)
	if err != nil {
		return nil, err
	}
	return ri.List(ctx, metav1.ListOptions{LabelSelector: labelSelector})
}

func (c *Client) Create(ctx context.Context, manager string, obj *unstructured.Unstructured) (*unstructured.Unstructured, error) {
	ri, err := c.getResourceInterface(obj.GroupVersionKind(), obj.GetNamespace())
	if err != nil {
		return nil, err
	}
	return ri.Create(ctx, obj, metav1.CreateOptions{FieldManager: manager})
}

func (c *Client) Replace(ctx context.Context, manager string, gvk schema.GroupVersionKind, ns string, name string, replace func(*unstructured.Unstructured) (*unstructured.Unstructured, error)) (*unstructured.Unstructured, error) {
	ri, err := c.getResourceInterface(gvk, ns)
	if err != nil {
		return nil, err
	}
	obj, err := ri.Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}
	resourceVersion := obj.GetResourceVersion()
	newObj, err := replace(obj)
	if err != nil {
		return nil, err
	}
	newObj.SetResourceVersion(resourceVersion)
	return ri.Update(ctx, newObj, metav1.UpdateOptions{FieldManager: manager})
}

func (c *Client) SSA(ctx context.Context, manager string, obj *unstructured.Unstructured) (*unstructured.Unstructured, error) {
	ri, err := c.getResourceInterface(obj.GroupVersionKind(), obj.GetNamespace())
	if err != nil {
		return nil, err
	}

	objData, err := runtime.Encode(unstructured.UnstructuredJSONScheme, obj)
	if err != nil {
		return nil, err
	}

	return ri.Patch(ctx, obj.GetName(), types.ApplyPatchType, objData, metav1.PatchOptions{FieldManager: manager})
}

func (c *Client) Watch(ctx context.Context, gvk schema.GroupVersionKind, ns string) (watch.Interface, error) {
	ri, err := c.getResourceInterface(gvk, ns)
	if err != nil {
		return nil, err
	}
	return ri.Watch(ctx, metav1.ListOptions{})
}

func NewClient(config *rest.Config) (*Client, error) {
	di, err := dynamic.NewForConfig(config)
	if err != nil {
		return nil, err
	}
	dc, err := discovery.NewDiscoveryClientForConfig(config)
	if err != nil {
		return nil, err
	}
	mapper := restmapper.NewDeferredDiscoveryRESTMapper(memory.NewMemCacheClient(dc))
	return &Client{
		di:     di,
		mapper: mapper,
	}, nil
}
