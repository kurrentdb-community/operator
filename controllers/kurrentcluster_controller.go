package controllers

import (
	"context"
	"fmt"
	"strconv"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"

	kurrentv1 "github.com/kurrentdb-community/operator/api/v1"
)

// KurrentClusterReconciler reconciles a KurrentCluster object
type KurrentClusterReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

//+kubebuilder:rbac:groups=community.kurrent.io,resources=kurrentclusters,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=community.kurrent.io,resources=kurrentclusters/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=community.kurrent.io,resources=kurrentclusters/finalizers,verbs=update
//+kubebuilder:rbac:groups=apps,resources=statefulsets,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups="",resources=services,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups="",resources=persistentvolumeclaims,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch;update;patch

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
func (r *KurrentClusterReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	// Fetch the KurrentCluster instance
	var kurrentCluster kurrentv1.KurrentCluster
	if err := r.Get(ctx, req.NamespacedName, &kurrentCluster); err != nil {
		if errors.IsNotFound(err) {
			// Request object not found, could have been deleted after reconcile request.
			// Owned objects are automatically garbage collected.
			logger.Info("KurrentCluster resource not found. Ignoring since object must be deleted")
			return ctrl.Result{}, nil
		}
		// Error reading the object - requeue the request.
		logger.Error(err, "Failed to get KurrentCluster")
		return ctrl.Result{}, err
	}

	// Set default values
	r.setDefaults(&kurrentCluster)

	// Handle TLS certificates
	if err := r.reconcileCertificates(ctx, &kurrentCluster); err != nil {
		logger.Error(err, "Failed to reconcile certificates")
		return ctrl.Result{}, err
	}

	// Handle Services
	if err := r.reconcileServices(ctx, &kurrentCluster); err != nil {
		logger.Error(err, "Failed to reconcile Services")
		return ctrl.Result{}, err
	}

	// Handle StatefulSet
	if err := r.reconcileStatefulSet(ctx, &kurrentCluster); err != nil {
		logger.Error(err, "Failed to reconcile StatefulSet")
		return ctrl.Result{}, err
	}

	// Handle Leader Election
	if err := r.reconcileLeader(ctx, &kurrentCluster); err != nil {
		logger.Error(err, "Failed to reconcile Leader")
		// Don't fail the whole reconciliation if leader discovery fails,
		// but requeue sooner to try again
		return ctrl.Result{RequeueAfter: time.Second * 30}, nil
	}

	// Update status
	if err := r.updateStatus(ctx, &kurrentCluster); err != nil {
		logger.Error(err, "Failed to update status")
		return ctrl.Result{}, err
	}

	return ctrl.Result{RequeueAfter: time.Minute * 5}, nil
}

func (r *KurrentClusterReconciler) setDefaults(cluster *kurrentv1.KurrentCluster) {
	if cluster.Spec.Size == 0 {
		cluster.Spec.Size = 3
	}
	if cluster.Spec.Image == "" {
		cluster.Spec.Image = "docker.kurrent.io/kurrent-latest/kurrentdb:latest"
	}
	if cluster.Spec.ImagePullPolicy == "" {
		cluster.Spec.ImagePullPolicy = corev1.PullIfNotPresent
	}
	if cluster.Spec.Storage.Size == "" {
		cluster.Spec.Storage.Size = "10Gi"
	}
	if len(cluster.Spec.Storage.AccessModes) == 0 {
		cluster.Spec.Storage.AccessModes = []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}
	}
	if cluster.Spec.Network.ServiceType == "" {
		cluster.Spec.Network.ServiceType = corev1.ServiceTypeClusterIP
	}
	if cluster.Spec.Network.ExternalPort == 0 {
		cluster.Spec.Network.ExternalPort = 2113
	}
	if cluster.Spec.Network.InternalPort == 0 {
		cluster.Spec.Network.InternalPort = 2113
	}
	if cluster.Spec.Network.GossipPort == 0 {
		cluster.Spec.Network.GossipPort = 2113
	}
	if cluster.Spec.TLS.Enabled && cluster.Spec.TLS.AutoGenerate {
		// TLS auto-generation is enabled by default
	}
}

func (r *KurrentClusterReconciler) reconcileCertificates(ctx context.Context, cluster *kurrentv1.KurrentCluster) error {
	if !cluster.Spec.TLS.Enabled || !cluster.Spec.TLS.AutoGenerate {
		return nil
	}

	// Create certificate secret if it doesn't exist
	secretName := fmt.Sprintf("%s-certs", cluster.Name)
	secret := &corev1.Secret{}
	if err := r.Get(ctx, types.NamespacedName{Name: secretName, Namespace: cluster.Namespace}, secret); err != nil {
		if errors.IsNotFound(err) {
			// TODO: Implement certificate generation logic
			// For now, create a placeholder secret
			secret = &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{
					Name:      secretName,
					Namespace: cluster.Namespace,
				},
				Type: corev1.SecretTypeTLS,
				Data: map[string][]byte{
					"ca.crt":  []byte("# CA certificate will be generated here"),
					"tls.crt": []byte("# TLS certificate will be generated here"),
					"tls.key": []byte("# TLS private key will be generated here"),
					"ca.key":  []byte("# CA private key will be generated here"),
				},
			}

			if err := controllerutil.SetControllerReference(cluster, secret, r.Scheme); err != nil {
				return err
			}

			if err := r.Create(ctx, secret); err != nil {
				return err
			}
		} else {
			return err
		}
	}

	return nil
}

func (r *KurrentClusterReconciler) reconcileServices(ctx context.Context, cluster *kurrentv1.KurrentCluster) error {
	// Create headless service for StatefulSet
	headlessService := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      fmt.Sprintf("%s-headless", cluster.Name),
			Namespace: cluster.Namespace,
		},
		Spec: corev1.ServiceSpec{
			ClusterIP:                "None",
			PublishNotReadyAddresses: true,
			Selector: map[string]string{
				"app.kubernetes.io/name":     "kurrentdb",
				"app.kubernetes.io/instance": cluster.Name,
			},
			Ports: []corev1.ServicePort{
				{
					Name:       "client",
					Port:       cluster.Spec.Network.InternalPort,
					TargetPort: intstr.FromInt(int(cluster.Spec.Network.InternalPort)),
					Protocol:   corev1.ProtocolTCP,
				},
			},
		},
	}

	if err := controllerutil.SetControllerReference(cluster, headlessService, r.Scheme); err != nil {
		return err
	}

	if err := r.createOrUpdateService(ctx, headlessService); err != nil {
		return err
	}

	// Create client service
	clientService := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      cluster.Name,
			Namespace: cluster.Namespace,
		},
		Spec: corev1.ServiceSpec{
			Type: cluster.Spec.Network.ServiceType,
			Selector: map[string]string{
				"app.kubernetes.io/name":     "kurrentdb",
				"app.kubernetes.io/instance": cluster.Name,
			},
			Ports: []corev1.ServicePort{
				{
					Name:       "client",
					Port:       cluster.Spec.Network.ExternalPort,
					TargetPort: intstr.FromInt(int(cluster.Spec.Network.InternalPort)),
					Protocol:   corev1.ProtocolTCP,
				},
			},
		},
	}

	if cluster.Spec.Network.Annotations != nil {
		clientService.Annotations = cluster.Spec.Network.Annotations
	}

	if err := controllerutil.SetControllerReference(cluster, clientService, r.Scheme); err != nil {
		return err
	}

	if err := r.createOrUpdateService(ctx, clientService); err != nil {
		return err
	}

	// Create leader service
	leaderService := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      fmt.Sprintf("%s-leader", cluster.Name),
			Namespace: cluster.Namespace,
		},
		Spec: corev1.ServiceSpec{
			Type: corev1.ServiceTypeClusterIP,
			Selector: map[string]string{
				"app.kubernetes.io/name":     "kurrentdb",
				"app.kubernetes.io/instance": cluster.Name,
				"community.kurrent.io/role":  "leader",
			},
			Ports: []corev1.ServicePort{
				{
					Name:       "client",
					Port:       cluster.Spec.Network.ExternalPort,
					TargetPort: intstr.FromInt(int(cluster.Spec.Network.InternalPort)),
					Protocol:   corev1.ProtocolTCP,
				},
			},
		},
	}

	if err := controllerutil.SetControllerReference(cluster, leaderService, r.Scheme); err != nil {
		return err
	}

	return r.createOrUpdateService(ctx, leaderService)
}

func (r *KurrentClusterReconciler) createOrUpdateService(ctx context.Context, service *corev1.Service) error {
	existing := &corev1.Service{}
	if err := r.Get(ctx, types.NamespacedName{Name: service.Name, Namespace: service.Namespace}, existing); err != nil {
		if errors.IsNotFound(err) {
			return r.Create(ctx, service)
		}
		return err
	}

	existing.Spec.Ports = service.Spec.Ports
	existing.Spec.Selector = service.Spec.Selector
	if service.Annotations != nil {
		existing.Annotations = service.Annotations
	}
	return r.Update(ctx, existing)
}

func (r *KurrentClusterReconciler) reconcileStatefulSet(ctx context.Context, cluster *kurrentv1.KurrentCluster) error {
	statefulSet := r.buildStatefulSet(cluster)

	if err := controllerutil.SetControllerReference(cluster, statefulSet, r.Scheme); err != nil {
		return err
	}

	existing := &appsv1.StatefulSet{}
	if err := r.Get(ctx, types.NamespacedName{Name: statefulSet.Name, Namespace: statefulSet.Namespace}, existing); err != nil {
		if errors.IsNotFound(err) {
			return r.Create(ctx, statefulSet)
		}
		return err
	}

	existing.Spec = statefulSet.Spec

	return r.Update(ctx, existing)
}

func (r *KurrentClusterReconciler) buildStatefulSet(cluster *kurrentv1.KurrentCluster) *appsv1.StatefulSet {
	labels := map[string]string{
		"app.kubernetes.io/name":     "kurrentdb",
		"app.kubernetes.io/instance": cluster.Name,
	}

	volumes := []corev1.Volume{}

	volumeMounts := []corev1.VolumeMount{
		{
			Name:      "data",
			MountPath: "/var/lib/eventstore",
		},
	}

	// Add TLS volumes if enabled
	if cluster.Spec.TLS.Enabled {
		secretName := cluster.Spec.TLS.CertificatesSecretName
		if secretName == "" {
			secretName = fmt.Sprintf("%s-certs", cluster.Name)
		}

		volumes = append(volumes, corev1.Volume{
			Name: "certs",
			VolumeSource: corev1.VolumeSource{
				Secret: &corev1.SecretVolumeSource{
					SecretName: secretName,
				},
			},
		})

		volumeMounts = append(volumeMounts, corev1.VolumeMount{
			Name:      "certs",
			MountPath: "/certs",
			ReadOnly:  true,
		})
	}

	env := r.buildEnvironmentVariables(cluster)

	container := corev1.Container{
		Name:            "kurrentdb",
		Image:           cluster.Spec.Image,
		ImagePullPolicy: cluster.Spec.ImagePullPolicy,
		Ports: []corev1.ContainerPort{
			{
				Name:          "client",
				ContainerPort: cluster.Spec.Network.InternalPort,
				Protocol:      corev1.ProtocolTCP,
			},
		},
		Env:          env,
		VolumeMounts: volumeMounts,
		Resources:    cluster.Spec.Resources,
		LivenessProbe: &corev1.Probe{
			ProbeHandler: corev1.ProbeHandler{
				HTTPGet: &corev1.HTTPGetAction{
					Path:   "/health/live",
					Port:   intstr.FromInt(int(cluster.Spec.Network.InternalPort)),
					Scheme: corev1.URISchemeHTTP,
				},
			},
			InitialDelaySeconds: 30,
			PeriodSeconds:       10,
			TimeoutSeconds:      5,
			FailureThreshold:    6,
		},
		ReadinessProbe: &corev1.Probe{
			ProbeHandler: corev1.ProbeHandler{
				HTTPGet: &corev1.HTTPGetAction{
					Path:   "/health/live",
					Port:   intstr.FromInt(int(cluster.Spec.Network.InternalPort)),
					Scheme: corev1.URISchemeHTTP,
				},
			},
			InitialDelaySeconds: 5,
			PeriodSeconds:       5,
			TimeoutSeconds:      5,
			FailureThreshold:    3,
		},
	}

	podSpec := corev1.PodSpec{
		Containers:         []corev1.Container{container},
		Volumes:            volumes,
		NodeSelector:       cluster.Spec.NodeSelector,
		Tolerations:        cluster.Spec.Tolerations,
		Affinity:           cluster.Spec.Affinity,
		ServiceAccountName: cluster.Spec.ServiceAccountName,
		TopologySpreadConstraints: []corev1.TopologySpreadConstraint{
			{
				MaxSkew:           1,
				TopologyKey:       "kubernetes.io/hostname",
				WhenUnsatisfiable: corev1.ScheduleAnyway,
				LabelSelector: &metav1.LabelSelector{
					MatchLabels: labels,
				},
			},
		},
	}

	statefulSet := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      cluster.Name,
			Namespace: cluster.Namespace,
			Labels:    labels,
		},
		Spec: appsv1.StatefulSetSpec{
			Replicas:            &cluster.Spec.Size,
			ServiceName:         fmt.Sprintf("%s-headless", cluster.Name),
			PodManagementPolicy: appsv1.ParallelPodManagement,
			UpdateStrategy: appsv1.StatefulSetUpdateStrategy{
				Type: appsv1.RollingUpdateStatefulSetStrategyType,
				RollingUpdate: &appsv1.RollingUpdateStatefulSetStrategy{
					MaxUnavailable: &intstr.IntOrString{Type: intstr.Int, IntVal: 1},
				},
			},
			Selector: &metav1.LabelSelector{
				MatchLabels: labels,
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: labels,
				},
				Spec: podSpec,
			},
			VolumeClaimTemplates: []corev1.PersistentVolumeClaim{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: "data",
					},
					Spec: corev1.PersistentVolumeClaimSpec{
						AccessModes: cluster.Spec.Storage.AccessModes,
						Resources: corev1.ResourceRequirements{
							Requests: corev1.ResourceList{
								corev1.ResourceStorage: resource.MustParse(cluster.Spec.Storage.Size),
							},
						},
						StorageClassName: cluster.Spec.Storage.StorageClassName,
					},
				},
			},
		},
	}

	return statefulSet
}

func (r *KurrentClusterReconciler) buildEnvironmentVariables(cluster *kurrentv1.KurrentCluster) []corev1.EnvVar {
	env := []corev1.EnvVar{
		{
			Name: "KURRENTDB_REPLICATION_IP",
			ValueFrom: &corev1.EnvVarSource{
				FieldRef: &corev1.ObjectFieldSelector{
					FieldPath: "status.podIP",
				},
			},
		},
		{
			Name: "KURRENTDB_NODE_HOST_ADVERTISE_AS",
			ValueFrom: &corev1.EnvVarSource{
				FieldRef: &corev1.ObjectFieldSelector{
					FieldPath: "status.podIP",
				},
			},
		},
		{
			Name:  "KURRENTDB_CLUSTER_SIZE",
			Value: strconv.Itoa(int(cluster.Spec.Size)),
		},
		{
			Name:  "KURRENTDB_INSECURE",
			Value: "true",
		},
		{
			Name:  "KURRENTDB_ENABLE_ATOM_PUB_OVER_HTTP",
			Value: "true",
		},
	}

	serviceName := fmt.Sprintf("%s-headless", cluster.Name)
	env = append(env,
		corev1.EnvVar{
			Name:  "KURRENTDB_DISCOVER_VIA_DNS",
			Value: "true",
		},
		corev1.EnvVar{
			Name:  "KURRENTDB_CLUSTER_DNS",
			Value: fmt.Sprintf("%s.%s.svc.cluster.local", serviceName, cluster.Namespace),
		},
	)

	if cluster.Spec.TLS.Enabled {
		env = append(env, []corev1.EnvVar{
			{
				Name:  "KURRENTDB_CERTIFICATE_FILE",
				Value: "/certs/tls.crt",
			},
			{
				Name:  "KURRENTDB_CERTIFICATE_PRIVATE_KEY_FILE",
				Value: "/certs/tls.key",
			},
			{
				Name:  "KURRENTDB_TRUSTED_ROOT_CERTIFICATES_PATH",
				Value: "/certs/ca.crt",
			},
		}...)
	}

	env = append(env, cluster.Spec.Environment...)

	return env
}

func (r *KurrentClusterReconciler) updateStatus(ctx context.Context, cluster *kurrentv1.KurrentCluster) error {
	// Get the StatefulSet to check status
	statefulSet := &appsv1.StatefulSet{}
	if err := r.Get(ctx, types.NamespacedName{Name: cluster.Name, Namespace: cluster.Namespace}, statefulSet); err != nil {
		return err
	}

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

	nodeStatuses := []kurrentv1.NodeStatus{}
	for _, pod := range podList.Items {
		ready := false
		for _, cond := range pod.Status.Conditions {
			if cond.Type == corev1.PodReady && cond.Status == corev1.ConditionTrue {
				ready = true
				break
			}
		}

		nodeStatuses = append(nodeStatuses, kurrentv1.NodeStatus{
			Name:  pod.Name,
			Ready: ready,
			Role:  pod.Labels["community.kurrent.io/role"],
		})
	}

	// Update status
	cluster.Status.Replicas = statefulSet.Status.Replicas
	cluster.Status.ReadyReplicas = statefulSet.Status.ReadyReplicas
	cluster.Status.ObservedGeneration = cluster.Generation
	cluster.Status.Nodes = nodeStatuses

	// Determine phase
	if cluster.Status.ReadyReplicas == cluster.Spec.Size {
		cluster.Status.Phase = kurrentv1.ClusterPhaseRunning
	} else if cluster.Status.Replicas > 0 {
		cluster.Status.Phase = kurrentv1.ClusterPhaseCreating
	} else {
		cluster.Status.Phase = kurrentv1.ClusterPhaseCreating
	}

	return r.Status().Update(ctx, cluster)
}

// SetupWithManager sets up the controller with the Manager.
func (r *KurrentClusterReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&kurrentv1.KurrentCluster{}).
		Owns(&appsv1.StatefulSet{}).
		Owns(&corev1.Service{}).
		Owns(&corev1.Secret{}).
		Complete(r)
}
