import os, sys
import shutil
from pathlib import Path
from metasmith.python_api import Agent, TargetBuilder, Source, SshSource, DataInstanceLibrary, TransformInstanceLibrary, DataTypeLibrary
from metasmith.python_api import Resources, Size, Duration
from metasmith.python_api import ContainerRuntime
from metasmith.python_api import WorkflowTask
from metasmith.hashing import KeyGenerator

base_dir = Path("./cache")
# with open("../../secrets/slurm_account_fir") as f:
with open("../../secrets/slurm_account_sockeye") as f:
    SLURM_ACCOUNT = f.readline()

# agent_home = Source.FromLocal((Path("/home/tony/workspace/tools/MetasmithLibraries/tests/cache/local_home")).resolve())
# smith = Agent(
#     home = agent_home,
#     # runtime=ContainerRuntime.APPTAINER,
#     runtime=ContainerRuntime.DOCKER,
# )

agent_home = SshSource(host="sockeye", path=Path(f"/scratch/{SLURM_ACCOUNT}/pwy_group/metasmith")).AsSource()
smith = Agent(
    home = agent_home,
    runtime=ContainerRuntime.APPTAINER,
    setup_commands=[
        'module load gcc/9.4.0',
        'module load apptainer/1.3.1',
    ]
)

# agent_home = SshSource("fir", Path("/scratch/phyberos/metasmith")).AsSource()
# smith = Agent(
#     home = agent_home,
#     setup_commands=[
#         "module load StdEnv/2023",
#         "module load apptainer/1.3.5",
#     ],
#     runtime=ContainerRuntime.APPTAINER,
# )

# smith.Deploy(assertive=True)

# import ipynbname
# notebook_name = ipynbname.name()
notebook_name = Path(__file__).stem
# data=Path("../data").resolve()
data=Path(f"/arc/project/{SLURM_ACCOUNT}/pwy_group/data/model_strains")
input_raw = [
    ((data/"Ana_PS.fastq.gz"), "sequences::long_reads", dict(parity="single", length_class="long")),
    ((data/"Nos_PS.fastq.gz"), "sequences::long_reads", dict(parity="single", length_class="long")),
    ((data/"SynC_PS.fastq.gz"), "sequences::long_reads", dict(parity="single", length_class="long")),
    ((data/"SynT_PS.fastq.gz"), "sequences::long_reads", dict(parity="single", length_class="long")),
]
_, _hash = KeyGenerator.FromStr("".join(str(p) for p, t, m in input_raw))
in_dir = base_dir/f"{notebook_name}/inputs.{_hash}.xgdb"
todo = {}
for p, t, m in input_raw:
    if isinstance(p, Path):
        meta = Path(f"{p.name}.json")
        reads = p
        # reads = Path(p.name)
    else:
        k = p
        meta = Path(f"{p}.json")
        reads = Path(f"{p}.acc")
    todo[p] = {meta, reads}

lib_path = Path("/home/tony/workspace/tools/MetasmithLibraries")
d_path = lib_path/"data_types"
if in_dir.exists() and False:
    inputs = DataInstanceLibrary.Load(in_dir)
else:
    inputs = DataInstanceLibrary(in_dir)
    inputs.Purge()
    inputs.AddTypeLibrary("sequences", DataTypeLibrary.Load(d_path/"sequences.yml"))
    inputs.AddTypeLibrary("ncbi", DataTypeLibrary.Load(d_path/"ncbi.yml"))
    for p, t, m in input_raw:
        if isinstance(p, Path):
            meta = inputs.AddValue(f"{p.name}.json", m, "sequences::read_metadata")
            # shutil.copy2(p, inputs.location/p.name)
            # p = Path(p.name)
            reads = inputs.AddItem(p, t, parents={meta})
        else:
            k = p
            meta = inputs.AddValue(f"{p}.json", m, "sequences::read_metadata")
            reads = inputs.AddValue(f"{p}.acc", p, t, parents={meta})
    inputs.Save()

# inputs = DataInstanceLibrary.Load(in_dir)

resources = [
    DataInstanceLibrary.Load(lib_path/f"resources/{n}")
    for n in [
        "containers",
        # "lib",
    ]
]

transforms = [
    TransformInstanceLibrary.Load(lib_path/f"transforms/{n}")
    for n in [
        "logistics",
        "assembly",
    ]
]

# lib = TransformInstanceLibrary.Load(lib_path/f"transforms/assembly")
# k = Path("flye.py")
# if k in lib.manifest:
#     del lib.manifest[k]
# transforms.append(lib)

for lib in transforms:
    print(lib.manifest)

targets = TargetBuilder()
for n, p in [
        # "sequences::miniasm_gfa",
        ("sequences::read_qc_stats",                set()),
        ("sequences::flye_raw_assembly",            set()),
        ("sequences::assembly_stats",               {"sequences::flye_raw_assembly"}),
        ("sequences::assembly_per_bp_coverage",     {"sequences::flye_raw_assembly"}),
        ("sequences::assembly_per_contig_coverage", {"sequences::flye_raw_assembly"}),
    ]:
    targets.Add(n, p)

task = smith.GenerateWorkflow(
    samples=[inputs.AsView(mask=v) for k, v in todo.items()],
    resources=resources,
    transforms=transforms,
    # targets=["sequences::read_qc_stats"],
    targets=targets,
)
# task.SaveAs(Source.FromLocal(Path("./cache/test.task").absolute()))
# p = task.plan._solver_result.RenderDAG(base_dir/f"{notebook_name}/dag_raw")
p = task.plan.RenderDAG(base_dir/f"{notebook_name}/dag")
print(task.ok, len(task.plan.steps))
print(p)
print(f"task: {task.GetKey()}, input {in_dir}")

tpath = Path("./cache/test").absolute()
task.SaveAs(Source.FromLocal(tpath))
WorkflowTask.Load(tpath)

smith.StageWorkflow(task, on_exist="update_all", verify_external_paths=True)
# smith.StageWorkflow(task, on_exist="clear", verify_external_paths=False)


params = dict(
    slurmAccount=SLURM_ACCOUNT,
    # executor=dict(
    #     queueSize=500,
    # ),
    process=dict(
        array=2,
        tries=2,
    )
)
smith.RunWorkflow(
    task=task,
    config_file=smith.GetNxfConfigPresets()["slurm"],
    # config_file=Path("./fir_config.nf"),
    # config_file=smith.GetNxfConfigPresets()["local"],
    params=params,
    # resource_overrides={
    #     "all": Resources(
    #         memory=Size.MB(1),
    #         cpus=5,
    #     ),
    #     transforms[1]["megahit.py"]: Resources(
    #         cpus=15,
    #     )
    # }
)
