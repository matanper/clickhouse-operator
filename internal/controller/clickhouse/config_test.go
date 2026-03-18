package clickhouse

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"gopkg.in/yaml.v2"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/utils/ptr"

	v1 "github.com/ClickHouse/clickhouse-operator/api/v1alpha1"
)

var _ = Describe("ConfigGenerator", func() {
	ctx := clickhouseReconciler{
		reconcilerBase: reconcilerBase{
			Cluster: &v1.ClickHouseCluster{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-cluster",
					Namespace: "test-namespace",
				},
				Spec: v1.ClickHouseClusterSpec{
					Replicas: ptr.To[int32](3),
					Shards:   ptr.To[int32](2),
					Settings: v1.ClickHouseSettings{
						ExtraConfig: runtime.RawExtension{
							Raw: []byte(`{"test": "value"}`),
						},
						ExtraUsersConfig: runtime.RawExtension{
							Raw: []byte(`{}`),
						},
					},
				},
			},
		},
		keeper: v1.KeeperCluster{
			Spec: v1.KeeperClusterSpec{
				Replicas: ptr.To[int32](3),
			},
		},
	}

	for _, generator := range generators {
		gen := generator
		It("should generate config: "+gen.Filename(), func() {
			if !gen.Exists(&ctx) {
				Skip("generator does not apply to this cluster spec")
			}
			data, err := gen.Generate(&ctx, v1.ClickHouseReplicaID{})
			Expect(err).ToNot(HaveOccurred())

			obj := map[any]any{}
			Expect(yaml.Unmarshal([]byte(data), &obj)).To(Succeed())
		})
	}

	It("should generate storage JBOD config when additionalDataVolumeClaimSpecs is set", func() {
		ctxJBOD := clickhouseReconciler{
			reconcilerBase: reconcilerBase{
				Cluster: &v1.ClickHouseCluster{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "test-jbod",
						Namespace: "test-namespace",
					},
					Spec: v1.ClickHouseClusterSpec{
						Replicas: ptr.To[int32](2),
						Shards:   ptr.To[int32](1),
						KeeperClusterRef: &corev1.LocalObjectReference{Name: "keeper"},
						AdditionalDataVolumeClaimSpecs: []v1.AdditionalVolumeClaimSpec{
							{Name: "disk1", Spec: corev1.PersistentVolumeClaimSpec{}},
							{Name: "disk2", MountPath: "/custom/path", Spec: corev1.PersistentVolumeClaimSpec{}},
						},
					},
				},
			},
			keeper: v1.KeeperCluster{Spec: v1.KeeperClusterSpec{Replicas: ptr.To[int32](3)}},
		}
		ctxJBOD.Cluster.Spec.WithDefaults()

		configData, err := generateConfigForSingleReplica(&ctxJBOD, v1.ClickHouseReplicaID{})
		Expect(err).ToNot(HaveOccurred())

		storageConfig, ok := configData["etc-clickhouse-server-config-d-10-storage-jbod-yaml"]
		Expect(ok).To(BeTrue())
		Expect(storageConfig).To(ContainSubstring("storage_configuration"))
		Expect(storageConfig).To(ContainSubstring("default"))
		Expect(storageConfig).To(ContainSubstring("disk1"))
		Expect(storageConfig).To(ContainSubstring("disk2"))
		Expect(storageConfig).To(ContainSubstring("/var/lib/clickhouse/disks/disk1/"))
		Expect(storageConfig).To(ContainSubstring("/custom/path/"))
	})
})
