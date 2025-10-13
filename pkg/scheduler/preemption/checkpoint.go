package preemption

import (
	"context"
	"fmt"
	"strings"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/klog/v2"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta1"
)

const (
	checkpointBackupAPIVersion   = "migration.dcnlab.com/v1"
	checkpointBackupKind         = "CheckpointBackup"
	checkpointBackupSchedule     = "immediately"
	checkpointRegistryURL        = "http://192.168.40.246:30080"
	checkpointRegistryRepository = "checkpoint"
	checkpointRegistrySecretName = "backup-registry-harbor-local-registry-1759822477"
	checkpointRegistrySecretNS   = "stateful-migration"
	checkpointBackupNamePrefix   = "checkpoint-"
	maxDNS1123LabelLength        = validation.DNS1123LabelMaxLength
	checkpointPhaseCheckpointed        = "Checkpointed"
	checkpointPhaseImageBuilding       = "ImageBuilding"
	checkpointPhaseImageBuilt          = "ImageBuilt"
	checkpointPhaseImagePushing        = "ImagePushing"
	checkpointPhaseImagePushed         = "ImagePushed"
	checkpointPhaseCompleted           = "Completed"
	checkpointPhaseCompletedPodDeleted = "CompletedPodDeleted"
	checkpointPhaseFailed              = "Failed"
	checkpointWaitInterval             = 2 * time.Second
	checkpointWaitTimeout              = 2 * time.Minute
)

func (p *Preemptor) ensureCheckpointBackup(ctx context.Context, w *kueue.Workload) error {
	log := ctrl.LoggerFrom(ctx)

	owner := metav1.GetControllerOf(w)
	if owner == nil {
		return fmt.Errorf("workload %s/%s has no controller owner", w.Namespace, w.Name)
	}
	if owner.Kind != "Job" || !strings.HasPrefix(owner.APIVersion, "batch/") {
		return fmt.Errorf("workload %s/%s has unsupported owner %s %s", w.Namespace, w.Name, owner.APIVersion, owner.Kind)
	}

	job := &batchv1.Job{}
	jobKey := client.ObjectKey{Name: owner.Name, Namespace: w.Namespace}
	if err := p.client.Get(ctx, jobKey, job); err != nil {
		return fmt.Errorf("getting job %s/%s for workload %s: %w", w.Namespace, owner.Name, w.Name, err)
	}

	backupName := checkpointBackupName(w.Name)
	existing := &unstructured.Unstructured{}
	existing.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "migration.dcnlab.com",
		Version: "v1",
		Kind:    checkpointBackupKind,
	})
	if err := p.client.Get(ctx, client.ObjectKey{Name: backupName, Namespace: job.Namespace}, existing); err == nil {
		log.V(4).Info("CheckpointBackup already exists", "name", backupName, "namespace", job.Namespace)
		return p.waitCheckpointCompletion(ctx, job.Namespace, backupName)
	} else if !apierrors.IsNotFound(err) {
		return fmt.Errorf("checking existing CheckpointBackup %s/%s: %w", job.Namespace, backupName, err)
	}

	pod, err := p.selectJobPod(ctx, job)
	if err != nil {
		return err
	}

	containers := make([]interface{}, 0, len(pod.Spec.Containers))
	for range pod.Spec.Containers {
		containers = append(containers, map[string]interface{}{
			"name":  pod.Name,
			"image": fmt.Sprintf("checkpoint/%s-is-checkpointed", pod.Name),
		})
	}

	backup := &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": checkpointBackupAPIVersion,
			"kind":       checkpointBackupKind,
			"metadata": map[string]interface{}{
				"name":      backupName,
				"namespace": job.Namespace,
				"labels": map[string]interface{}{
					"kueue.x-k8s.io/workload-name": w.Name,
				},
			},
			"spec": map[string]interface{}{
				"schedule": checkpointBackupSchedule,
				"stopPod":  false,
				"podRef": map[string]interface{}{
					"name":      pod.Name,
					"namespace": pod.Namespace,
				},
				"resourceRef": map[string]interface{}{
					"apiVersion": owner.APIVersion,
					"kind":       owner.Kind,
					"name":       job.Name,
					"namespace":  job.Namespace,
				},
				"registry": map[string]interface{}{
					"url":        checkpointRegistryURL,
					"repository": checkpointRegistryRepository,
					"secretRef": map[string]interface{}{
						"name":      checkpointRegistrySecretName,
						"namespace": checkpointRegistrySecretNS,
					},
				},
				"containers": containers,
			},
		},
	}
	backup.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "migration.dcnlab.com",
		Version: "v1",
		Kind:    checkpointBackupKind,
	})

	if err := p.client.Create(ctx, backup); err != nil {
		if apierrors.IsAlreadyExists(err) {
			log.V(4).Info("CheckpointBackup already exists on create", "name", backupName, "namespace", job.Namespace)
			return p.waitCheckpointCompletion(ctx, job.Namespace, backupName)
		}
		return fmt.Errorf("creating CheckpointBackup %s/%s: %w", job.Namespace, backupName, err)
	}
	log.V(3).Info("Created CheckpointBackup for preempted workload", "workload", klog.KObj(w), "checkpointBackup", klog.KRef(job.Namespace, backupName))
	return p.waitCheckpointCompletion(ctx, job.Namespace, backupName)
}

func (p *Preemptor) selectJobPod(ctx context.Context, job *batchv1.Job) (*corev1.Pod, error) {
	podList := &corev1.PodList{}
	if err := p.client.List(ctx, podList, client.InNamespace(job.Namespace), client.MatchingLabels(map[string]string{batchv1.JobNameLabel: job.Name})); err != nil {
		return nil, fmt.Errorf("listing pods for job %s/%s: %w", job.Namespace, job.Name, err)
	}
	if len(podList.Items) == 0 {
		return nil, fmt.Errorf("no pods found for job %s/%s", job.Namespace, job.Name)
	}

	var running, pending, fallback *corev1.Pod
	for i := range podList.Items {
		pod := &podList.Items[i]
		if pod.DeletionTimestamp != nil {
			continue
		}
		switch pod.Status.Phase {
		case corev1.PodRunning:
			if running == nil || pod.CreationTimestamp.After(running.CreationTimestamp.Time) {
				running = pod
			}
		case corev1.PodPending:
			if pending == nil || pod.CreationTimestamp.After(pending.CreationTimestamp.Time) {
				pending = pod
			}
		default:
			if fallback == nil || pod.CreationTimestamp.After(fallback.CreationTimestamp.Time) {
				fallback = pod
			}
		}
	}
	if running != nil {
		return running, nil
	}
	if pending != nil {
		return pending, nil
	}
	if fallback != nil {
		return fallback, nil
	}
	return nil, fmt.Errorf("no suitable pods found for job %s/%s", job.Namespace, job.Name)
}

func checkpointBackupName(workloadName string) string {
	name := checkpointBackupNamePrefix + workloadName
	if len(name) <= maxDNS1123LabelLength {
		return name
	}
	name = name[:maxDNS1123LabelLength]
	name = strings.TrimRight(name, "-")
	if name == "" {
		return checkpointBackupNamePrefix + "backup"
	}
	return name
}

func (p *Preemptor) waitForCheckpointCompletion(ctx context.Context, namespace, name string) error {
	log := ctrl.LoggerFrom(ctx)
	return wait.PollUntilContextTimeout(ctx, checkpointWaitInterval, checkpointWaitTimeout, true, func(ctx context.Context) (bool, error) {
		backup := &unstructured.Unstructured{}
		backup.SetGroupVersionKind(schema.GroupVersionKind{
			Group:   "migration.dcnlab.com",
			Version: "v1",
			Kind:    checkpointBackupKind,
		})
		if err := p.client.Get(ctx, client.ObjectKey{Name: name, Namespace: namespace}, backup); err != nil {
			if apierrors.IsNotFound(err) {
				log.V(4).Info("CheckpointBackup not found yet", "name", name, "namespace", namespace)
				return false, nil
			}
			return false, err
		}

		phase, found, err := unstructured.NestedString(backup.Object, "status", "phase")
		if err != nil {
			return false, fmt.Errorf("reading status.phase for CheckpointBackup %s/%s: %w", namespace, name, err)
		}
		if !found || phase == "" {
			log.V(4).Info("CheckpointBackup still processing", "name", name, "namespace", namespace)
			return false, nil
		}
		switch phase {
		case checkpointPhaseCheckpointed,
			checkpointPhaseImageBuilding,
			checkpointPhaseImageBuilt,
			checkpointPhaseImagePushing,
			checkpointPhaseImagePushed,
			checkpointPhaseCompleted,
			checkpointPhaseCompletedPodDeleted:
			log.V(3).Info("CheckpointBackup completed phase", "name", name, "namespace", namespace, "phase", phase)
			return true, nil
		case checkpointPhaseFailed:
			return false, fmt.Errorf("checkpoint backup %s/%s failed", namespace, name)
		default:
			log.V(4).Info("CheckpointBackup progressing", "name", name, "namespace", namespace, "phase", phase)
			return false, nil
		}
	})
}
