package controller

import (
	"context"
	"fmt"
	"reflect"
	"slices"
	"time"

	"github.com/blang/semver/v4"
	gcmp "github.com/google/go-cmp/cmp"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/selection"
	"k8s.io/apimachinery/pkg/types"
	ctrlruntime "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1 "github.com/ClickHouse/clickhouse-operator/api/v1alpha1"
	util "github.com/ClickHouse/clickhouse-operator/internal/controllerutil"
)

// ReplicaUpdateStage represents the stage of updating a ClickHouse replica. Used in reconciliation process.
type ReplicaUpdateStage int

const (
	StageUpToDate ReplicaUpdateStage = iota
	StageHasDiff
	StageNotReadyUpToDate
	StageUpdating
	StageError
	StageNotExists
)

var mapStatusText = map[ReplicaUpdateStage]string{
	StageUpToDate:         "UpToDate",
	StageHasDiff:          "HasDiff",
	StageNotReadyUpToDate: "NotReadyUpToDate",
	StageUpdating:         "Updating",
	StageError:            "Error",
	StageNotExists:        "NotExists",
}

func (s ReplicaUpdateStage) String() string {
	return mapStatusText[s]
}

var podErrorStatuses = []string{"ImagePullBackOff", "ErrImagePull", "CrashLoopBackOff", "CreateContainerError", "CreateContainerConfigError", "InvalidImageName"}

// CheckPodError checks if the pod of the given StatefulSet have permanent errors preventing it from starting.
func CheckPodError(ctx context.Context, log util.Logger, client client.Client, sts *appsv1.StatefulSet) (bool, error) {
	var pod corev1.Pod

	podName := sts.Name + "-0"

	if err := client.Get(ctx, types.NamespacedName{
		Namespace: sts.Namespace,
		Name:      podName,
	}, &pod); err != nil {
		if !k8serrors.IsNotFound(err) {
			return false, fmt.Errorf("get clickhouse pod %q: %w", podName, err)
		}

		log.Info("pod does not exist", "pod", podName, "statefulset", sts.Name)

		return false, nil
	}

	isError := false
	for _, status := range pod.Status.ContainerStatuses {
		if status.State.Waiting != nil && slices.Contains(podErrorStatuses, status.State.Waiting.Reason) {
			log.Info("pod in error state", "pod", podName, "reason", status.State.Waiting.Reason)

			isError = true
			break
		}
	}

	return isError, nil
}

func diffFilter(specFields []string) gcmp.Option {
	return gcmp.FilterPath(func(path gcmp.Path) bool {
		inMeta := false
		for _, s := range path {
			if f, ok := s.(gcmp.StructField); ok {
				switch {
				case inMeta:
					return !slices.Contains([]string{"Labels", "Annotations"}, f.Name())
				case f.Name() == "ObjectMeta":
					inMeta = true
				default:
					return !slices.Contains(specFields, f.Name())
				}
			}
		}

		return false
	}, gcmp.Ignore())
}

func (r *ResourceReconcilerBase[Status, T, ReplicaID, S]) reconcileResource(
	ctx context.Context,
	log util.Logger,
	resource client.Object,
	specFields []string,
	action v1.EventAction,
) (bool, error) {
	cli := r.GetClient()
	kind := resource.GetObjectKind().GroupVersionKind().Kind
	log = log.With(kind, resource.GetName())

	if err := ctrlruntime.SetControllerReference(r.Cluster, resource, r.GetScheme()); err != nil {
		return false, fmt.Errorf("set %s/%s Ctrl reference: %w", kind, resource.GetName(), err)
	}

	if len(specFields) == 0 {
		return false, fmt.Errorf("%s specFields is empty", kind)
	}

	resourceHash, err := util.DeepHashResource(resource, specFields)
	if err != nil {
		return false, fmt.Errorf("deep hash %s/%s: %w", kind, resource.GetName(), err)
	}

	util.AddHashWithKeyToAnnotations(resource, util.AnnotationSpecHash, resourceHash)

	foundResource := resource.DeepCopyObject().(client.Object) //nolint:forcetypeassert // safe cast

	err = cli.Get(ctx, types.NamespacedName{
		Namespace: resource.GetNamespace(),
		Name:      resource.GetName(),
	}, foundResource)
	if err != nil {
		if !k8serrors.IsNotFound(err) {
			return false, fmt.Errorf("get %s/%s: %w", kind, resource.GetName(), err)
		}

		log.Info("resource not found, creating")

		return true, r.Create(ctx, resource, action)
	}

	if util.GetSpecHashFromObject(foundResource) == resourceHash {
		log.Debug("resource is up to date")
		return false, nil
	}

	log.Debug("resource changed, diff: " + gcmp.Diff(foundResource, resource, diffFilter(specFields)))

	foundResource.SetAnnotations(resource.GetAnnotations())
	foundResource.SetLabels(resource.GetLabels())

	for _, fieldName := range specFields {
		field := reflect.ValueOf(foundResource).Elem().FieldByName(fieldName)
		if !field.IsValid() || !field.CanSet() {
			panic("invalid data field  " + fieldName)
		}

		field.Set(reflect.ValueOf(resource).Elem().FieldByName(fieldName))
	}

	return true, r.Update(ctx, foundResource, action)
}

// ReconcileService reconciles a Kubernetes Service resource.
func (r *ResourceReconcilerBase[Status, T, ReplicaID, S]) ReconcileService(
	ctx context.Context,
	log util.Logger,
	service *corev1.Service,
	action v1.EventAction,
) (bool, error) {
	return r.reconcileResource(ctx, log, service, []string{"Spec"}, action)
}

// ReconcilePodDisruptionBudget reconciles a Kubernetes PodDisruptionBudget resource.
func (r *ResourceReconcilerBase[Status, T, ReplicaID, S]) ReconcilePodDisruptionBudget(
	ctx context.Context,
	log util.Logger,
	pdb *policyv1.PodDisruptionBudget,
	action v1.EventAction,
) (bool, error) {
	return r.reconcileResource(ctx, log, pdb, []string{"Spec"}, action)
}

// ReconcileConfigMap reconciles a Kubernetes ConfigMap resource.
func (r *ResourceReconcilerBase[Status, T, ReplicaID, S]) ReconcileConfigMap(
	ctx context.Context,
	log util.Logger,
	configMap *corev1.ConfigMap,
	action v1.EventAction,
) (bool, error) {
	return r.reconcileResource(ctx, log, configMap, []string{"Data", "BinaryData"}, action)
}

// ReconcilePVC reconciles a Kubernetes PersistentVolumeClaim resource.
// For bound PVCs the spec is largely immutable; only spec.resources.requests.storage
// and metadata (labels) are patched. All other spec fields are left untouched.
func (r *ResourceReconcilerBase[Status, T, ReplicaID, S]) ReconcilePVC(
	ctx context.Context,
	log util.Logger,
	pvc *corev1.PersistentVolumeClaim,
	action v1.EventAction,
) (bool, error) {
	cli := r.GetClient()
	const kind = "PersistentVolumeClaim"
	log = log.With(kind, pvc.GetName())

	if err := ctrlruntime.SetControllerReference(r.Cluster, pvc, r.GetScheme()); err != nil {
		return false, fmt.Errorf("set %s/%s ctrl reference: %w", kind, pvc.GetName(), err)
	}

	existing := &corev1.PersistentVolumeClaim{}
	if err := cli.Get(ctx, types.NamespacedName{Namespace: pvc.GetNamespace(), Name: pvc.GetName()}, existing); err != nil {
		if !k8serrors.IsNotFound(err) {
			return false, fmt.Errorf("get %s/%s: %w", kind, pvc.GetName(), err)
		}
		log.Info("PVC not found, creating")
		return true, r.Create(ctx, pvc, action)
	}

	// storageClassName is immutable; warn and skip if it diverges.
	desiredClass := pvc.Spec.StorageClassName
	existingClass := existing.Spec.StorageClassName
	if desiredClass != nil && existingClass != nil && *desiredClass != *existingClass {
		log.Warn("PVC storageClassName is immutable and cannot be changed in-place; "+
			"delete and recreate the PVC to switch storage class",
			"existing", *existingClass, "desired", *desiredClass)
		r.GetRecorder().Eventf(r.Cluster, existing, corev1.EventTypeWarning, v1.EventReasonFailedUpdate, action,
			"PVC %s storageClassName is immutable (existing: %s, desired: %s); delete and recreate to change it",
			existing.GetName(), *existingClass, *desiredClass)
		return false, nil
	}

	desiredStorage := pvc.Spec.Resources.Requests[corev1.ResourceStorage]
	existingStorage := existing.Spec.Resources.Requests[corev1.ResourceStorage]
	storageChanged := desiredStorage.Cmp(existingStorage) != 0
	vacChanged := pvc.Spec.VolumeAttributesClassName != existing.Spec.VolumeAttributesClassName
	labelsChanged := !reflect.DeepEqual(pvc.GetLabels(), existing.GetLabels())

	if !storageChanged && !vacChanged && !labelsChanged {
		log.Debug("PVC is up to date")
		return false, nil
	}

	// Build a merge patch that only touches the mutable fields — leaving every
	// immutable spec field (VolumeName, VolumeMode, AccessModes, StorageClassName,
	// Selector, DataSource, …) completely untouched.
	base := existing.DeepCopy()
	existing.SetLabels(pvc.GetLabels())
	if storageChanged {
		log.Info("resizing PVC storage", "from", existingStorage.String(), "to", desiredStorage.String())
		if existing.Spec.Resources.Requests == nil {
			existing.Spec.Resources.Requests = make(corev1.ResourceList)
		}
		existing.Spec.Resources.Requests[corev1.ResourceStorage] = desiredStorage
	}
	if vacChanged {
		log.Info("updating PVC volumeAttributesClassName",
			"from", existing.Spec.VolumeAttributesClassName, "to", pvc.Spec.VolumeAttributesClassName)
		existing.Spec.VolumeAttributesClassName = pvc.Spec.VolumeAttributesClassName
	}

	if err := cli.Patch(ctx, existing, client.MergeFrom(base)); err != nil {
		recorder := r.GetRecorder()
		if util.ShouldEmitEvent(err) {
			recorder.Eventf(r.Cluster, existing, corev1.EventTypeWarning, v1.EventReasonFailedUpdate, action,
				"Update %s %s failed: %s", kind, existing.GetName(), err.Error())
		}
		return false, fmt.Errorf("patch %s/%s: %w", kind, existing.GetName(), err)
	}

	return true, nil
}

// Create creates the given Kubernetes resource and emits events on failure.
func (r *ResourceReconcilerBase[Status, T, ReplicaID, S]) Create(ctx context.Context, resource client.Object, action v1.EventAction) error {
	recorder := r.GetRecorder()
	kind := resource.GetObjectKind().GroupVersionKind().Kind

	if err := r.GetClient().Create(ctx, resource); err != nil {
		if util.ShouldEmitEvent(err) {
			recorder.Eventf(r.Cluster, resource, corev1.EventTypeWarning, v1.EventReasonFailedCreate, action,
				"Create %s %s failed: %s", kind, resource.GetName(), err.Error())
		}

		return fmt.Errorf("create %s/%s: %w", kind, resource.GetName(), err)
	}

	return nil
}

// Update updates the given Kubernetes resource and emits events on failure.
func (r *ResourceReconcilerBase[Status, T, ReplicaID, S]) Update(ctx context.Context, resource client.Object, action v1.EventAction) error {
	recorder := r.GetRecorder()
	kind := resource.GetObjectKind().GroupVersionKind().Kind

	if err := r.GetClient().Update(ctx, resource); err != nil {
		if util.ShouldEmitEvent(err) {
			recorder.Eventf(r.Cluster, resource, corev1.EventTypeWarning, v1.EventReasonFailedUpdate, action,
				"Update %s %s failed: %s", kind, resource.GetName(), err.Error())
		}

		return fmt.Errorf("update %s/%s: %w", kind, resource.GetName(), err)
	}

	return nil
}

// Delete deletes the given Kubernetes resource and emits events on failure.
func (r *ResourceReconcilerBase[Status, T, ReplicaID, S]) Delete(ctx context.Context, resource client.Object, action v1.EventAction) error {
	recorder := r.GetRecorder()
	kind := resource.GetObjectKind().GroupVersionKind().Kind

	if err := r.GetClient().Delete(ctx, resource); err != nil {
		if k8serrors.IsNotFound(err) {
			return nil
		}

		if util.ShouldEmitEvent(err) {
			recorder.Eventf(r.Cluster, resource, corev1.EventTypeWarning, v1.EventReasonFailedDelete, action,
				"Delete %s %s failed: %s", kind, resource.GetName(), err.Error())
		}

		return fmt.Errorf("delete %s/%s: %w", kind, resource.GetName(), err)
	}

	return nil
}

// UpdatePVC updates the PersistentVolumeClaim for the given replica ID if it exists and differs from the provided spec.
// When targetPVCName is non-empty and multiple PVCs exist (e.g. from additional volumeClaimTemplates),
// the PVC matching that name is updated. targetPVCName should be "<vctName>-<statefulsetName>-<ordinal>".
func (r *ResourceReconcilerBase[Status, T, ReplicaID, S]) UpdatePVC(
	ctx context.Context,
	log util.Logger,
	id ReplicaID,
	volumeSpec corev1.PersistentVolumeClaimSpec,
	targetPVCName string,
	action v1.EventAction,
) error {
	cli := r.GetClient()

	var pvcs corev1.PersistentVolumeClaimList

	req := util.AppRequirements(r.Cluster.GetNamespace(), r.Cluster.SpecificName())
	for k, v := range id.Labels() {
		idReq, _ := labels.NewRequirement(k, selection.Equals, []string{v})
		req.LabelSelector = req.LabelSelector.Add(*idReq)
	}

	log.Debug("listing replica PVCs", "replica_id", id, "selector", req.LabelSelector.String())

	if err := cli.List(ctx, &pvcs, req); err != nil {
		return fmt.Errorf("list replica %v PVCs: %w", id, err)
	}

	if len(pvcs.Items) == 0 {
		log.Info("no PVCs found for replica, skipping update", "replica_id", id)
		return nil
	}

	var pvc *corev1.PersistentVolumeClaim
	if len(pvcs.Items) == 1 {
		pvc = &pvcs.Items[0]
	} else if targetPVCName != "" {
		for i := range pvcs.Items {
			if pvcs.Items[i].Name == targetPVCName {
				pvc = &pvcs.Items[i]
				break
			}
		}
		if pvc == nil {
			pvcNames := make([]string, len(pvcs.Items))
			for i, p := range pvcs.Items {
				pvcNames[i] = p.Name
			}
			return fmt.Errorf("target PVC %q not found among replica %v PVCs: %v", targetPVCName, id, pvcNames)
		}
	} else {
		pvcNames := make([]string, len(pvcs.Items))
		for i, p := range pvcs.Items {
			pvcNames[i] = p.Name
		}
		return fmt.Errorf("found multiple PVCs for replica %v: %v", id, pvcNames)
	}

	if gcmp.Equal(pvc.Spec, volumeSpec) {
		log.Debug("replica PVC is up to date", "pvc", pvc.Name)
		return nil
	}

	targetSpec := volumeSpec.DeepCopy()
	if err := util.ApplyDefault(targetSpec, pvc.Spec); err != nil {
		return fmt.Errorf("apply patch to replica PVC %v: %w", id, err)
	}

	log.Info("updating replica PVC", "pvc", pvc.Name, "diff", gcmp.Diff(pvc.Spec, targetSpec))

	pvc.Spec = *targetSpec
	if err := r.Update(ctx, pvc, action); err != nil {
		return fmt.Errorf("update replica PVC %v: %w", id, err)
	}

	return nil
}

// ReplicaUpdateInput contains the parameters needed to reconcile a StatefulSet for a replica.
type ReplicaUpdateInput struct {
	ExistingSTS           *appsv1.StatefulSet
	DesiredConfigMap      *corev1.ConfigMap
	DesiredSTS            *appsv1.StatefulSet
	AdditionalPVCs        []*corev1.PersistentVolumeClaim
	HasError              bool
	ConfigurationRevision string
	StatefulSetRevision   string
	BreakingSTSVersion    semver.Version
}

// ReconcileReplicaResources reconciles a replica's ConfigMap, StatefulSet and PVC.
// Handling Pod restarts on config changes.
func (r *ResourceReconcilerBase[Status, T, ReplicaID, S]) ReconcileReplicaResources(
	ctx context.Context,
	log util.Logger,
	replicaID ReplicaID,
	input ReplicaUpdateInput,
) (*ctrlruntime.Result, error) {
	configChanged, err := r.ReconcileConfigMap(ctx, log, input.DesiredConfigMap, v1.EventActionReconciling)
	if err != nil {
		return nil, fmt.Errorf("update replica ConfigMap: %w", err)
	}

	statefulSet := input.DesiredSTS

	if err := ctrlruntime.SetControllerReference(r.Cluster, statefulSet, r.GetScheme()); err != nil {
		return nil, fmt.Errorf("set replica StatefulSet controller reference: %w", err)
	}

	if len(input.AdditionalPVCs) > 0 {
		pvcErr := r.reconcileAdditionalPVCs(ctx, log, input.AdditionalPVCs)
		if pvcErr != nil {
			return nil, fmt.Errorf("reconcile additional PVCs: %w", pvcErr)
		}
	}

	if input.ExistingSTS == nil {
		log.Info("replica StatefulSet not found, creating", "statefulset", statefulSet.Name)
		util.AddObjectConfigHash(statefulSet, input.ConfigurationRevision)
		util.AddHashWithKeyToAnnotations(statefulSet, util.AnnotationSpecHash, input.StatefulSetRevision)

		if err := r.Create(ctx, statefulSet, v1.EventActionReconciling); err != nil {
			return nil, fmt.Errorf("create replica: %w", err)
		}

		return &ctrlruntime.Result{RequeueAfter: RequeueOnRefreshTimeout}, nil
	}

	// Check if the StatefulSet is outdated and needs to be recreated
	v, err := semver.Parse(input.ExistingSTS.Annotations[util.AnnotationStatefulSetVersion])
	if err != nil || input.BreakingSTSVersion.GT(v) {
		log.Warn("removing StatefulSet because of a breaking change", "found_version", v.String(), "expected_version", input.BreakingSTSVersion.String())

		if err := r.Delete(ctx, input.ExistingSTS, v1.EventActionReconciling); err != nil {
			return nil, fmt.Errorf("recreate replica: %w", err)
		}

		return &ctrlruntime.Result{RequeueAfter: RequeueOnRefreshTimeout}, nil
	}

	stsNeedsUpdate := util.GetSpecHashFromObject(input.ExistingSTS) != input.StatefulSetRevision

	// Trigger Pod restart if config changed
	// Always restarts on config changes, need to add check if it is needed.
	if util.GetConfigHashFromObject(input.ExistingSTS) != input.ConfigurationRevision {
		// Use same way as Kubernetes for force restarting Pods one by one
		// (https://github.com/kubernetes/kubernetes/blob/22a21f974f5c0798a611987405135ab7e62502da/staging/src/k8s.io/kubectl/pkg/polymorphichelpers/objectrestarter.go#L41)
		// Not included by default in the StatefulSet so that hash-diffs work correctly
		log.Info("forcing Pod restart, because of config changes")

		statefulSet.Spec.Template.Annotations[util.AnnotationRestartedAt] = time.Now().Format(time.RFC3339)

		util.AddObjectConfigHash(input.ExistingSTS, input.ConfigurationRevision)

		stsNeedsUpdate = true
	} else if restartedAt, ok := input.ExistingSTS.Spec.Template.Annotations[util.AnnotationRestartedAt]; ok {
		statefulSet.Spec.Template.Annotations[util.AnnotationRestartedAt] = restartedAt
	}

	if !stsNeedsUpdate {
		log.Debug("StatefulSet is up to date", "statefulset", statefulSet.Name)

		if configChanged {
			return &ctrlruntime.Result{RequeueAfter: RequeueOnRefreshTimeout}, nil
		}

		// Delete stuck pod if in error state so the StatefulSet controller can recreate it
		if input.HasError {
			podName := input.ExistingSTS.Name + "-0"
			pod := &corev1.Pod{}

			err = r.GetClient().Get(ctx, types.NamespacedName{Namespace: input.ExistingSTS.Namespace, Name: podName}, pod)
			if err != nil {
				if k8serrors.IsNotFound(err) {
					return &ctrlruntime.Result{RequeueAfter: RequeueOnRefreshTimeout}, nil
				}

				log.Warn("failed to get error pod", "pod", podName, "error", err)

				return &ctrlruntime.Result{RequeueAfter: RequeueOnRefreshTimeout}, nil
			}

			if pod.Labels[appsv1.ControllerRevisionHashLabelKey] != input.ExistingSTS.Status.UpdateRevision {
				log.Info("deleting pod stuck in error state", "pod", podName)

				if err = r.GetClient().Delete(ctx, pod); err != nil {
					log.Warn("failed to delete stuck pod", "pod", podName, "error", err)
				}

				return &ctrlruntime.Result{RequeueAfter: RequeueOnRefreshTimeout}, nil
			}
		}

		return nil, nil
	}

	if len(statefulSet.Spec.VolumeClaimTemplates) > 0 && len(input.ExistingSTS.Spec.VolumeClaimTemplates) > 0 {
		existingSpecsByTemplateName := make(map[string]corev1.PersistentVolumeClaimSpec, len(input.ExistingSTS.Spec.VolumeClaimTemplates))
		for _, template := range input.ExistingSTS.Spec.VolumeClaimTemplates {
			existingSpecsByTemplateName[template.Name] = template.Spec
		}

		for _, desiredTemplate := range statefulSet.Spec.VolumeClaimTemplates {
			existingSpec, ok := existingSpecsByTemplateName[desiredTemplate.Name]
			if !ok || gcmp.Equal(existingSpec, desiredTemplate.Spec) {
				continue
			}

			// Every replica StatefulSet has a single Pod with ordinal 0.
			pvcName := desiredTemplate.Name + "-" + input.ExistingSTS.Name + "-0"
			if err = r.UpdatePVC(ctx, log, replicaID, desiredTemplate.Spec, pvcName, v1.EventActionReconciling); err != nil {
				//nolint:nilerr // Error is logged internally and event sent
				return nil, nil
			}
		}

		// volumeClaimTemplates are immutable; always keep existing templates in StatefulSet updates.
		statefulSet.Spec.VolumeClaimTemplates = input.ExistingSTS.Spec.VolumeClaimTemplates
	}

	log.Info("updating replica StatefulSet", "statefulset", statefulSet.Name)
	log.Debug("replica StatefulSet diff", "diff", gcmp.Diff(input.ExistingSTS.Spec, statefulSet.Spec))
	input.ExistingSTS.Spec = statefulSet.Spec
	input.ExistingSTS.Annotations = util.MergeMaps(input.ExistingSTS.Annotations, statefulSet.Annotations)
	input.ExistingSTS.Labels = util.MergeMaps(input.ExistingSTS.Labels, statefulSet.Labels)
	util.AddHashWithKeyToAnnotations(input.ExistingSTS, util.AnnotationSpecHash, input.StatefulSetRevision)

	if err = r.Update(ctx, input.ExistingSTS, v1.EventActionReconciling); err != nil {
		return nil, fmt.Errorf("update replica: %w", err)
	}

	return &ctrlruntime.Result{RequeueAfter: RequeueOnRefreshTimeout}, nil
}

func (r *ResourceReconcilerBase[Status, T, ReplicaID, S]) reconcileAdditionalPVCs(
	ctx context.Context,
	log util.Logger,
	pvcs []*corev1.PersistentVolumeClaim,
) error {

	for _, desiredPVC := range pvcs {
		if desiredPVC == nil {
			continue
		}

		_, err := r.ReconcilePVC(ctx, log, desiredPVC, v1.EventActionReconciling)
		if err != nil {
			return err
		}
	}

	return nil
}
