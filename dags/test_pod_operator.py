from airflow.decorators import dag
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from pendulum import datetime

@dag(
    dag_id="test_kubernetes_pod_operator",
    start_date=datetime(2023, 1, 1),
    schedule=None,
    catchup=False,
    tags=["k8s", "pod-operator", "example"],
)
def test_k8s_pod_dag():
    """
    DAG ini mendemonstrasikan cara kerja KubernetesPodOperator.
    Ini adalah 'Container as a Task' yang murni.
    """

    # Task ini akan membuat Pod baru HANYA untuk menjalankan perintah ini
    # di dalam gambar Docker 'busybox'
    task_container_sebagai_task = KubernetesPodOperator(
        task_id="task_halo_dari_pod",
        name="pod-halo-dunia",  # Nama Pod K8s yang akan dibuat
        image="busybox",         # Gambar Docker yang akan digunakan (sangat ringan)
        cmds=["echo", "Halo, saya adalah task di dalam pod terpisah!"],
        get_logs=True,           # Ambil log dari pod
        is_delete_operator_pod=True, # Hapus pod setelah selesai (sukses atau gagal)
    )

# Panggil fungsi DAG
test_k8s_pod_dag()