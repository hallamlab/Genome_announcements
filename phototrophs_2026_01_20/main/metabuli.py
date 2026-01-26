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

host = "sockeye"
agent_home = SshSource(host=host, path=Path(f"/scratch/{SLURM_ACCOUNT}/pwy_group/metasmith")).AsSource()
smith = Agent(
    home = agent_home,
    runtime=ContainerRuntime.APPTAINER,
    setup_commands=[
        'module load gcc/9.4.0',
        'module load apptainer/1.3.1',
    ]
)

# host = "fir"
# agent_home = SshSource(host, Path("/scratch/phyberos/metasmith")).AsSource()
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
# data=Path("/arc/project/st-shallam-1/pwy_group/data/nostoc_anabaena_co-culture/flye_raw/sequences-flye_raw_assembly")
# data=Path("/arc/project/st-shallam-1/pwy_group/data/nostoc_anabaena_co-culture/hifi_100x/sequences-isolate_assembly")
data=Path("/arc/project/st-shallam-1/pwy_group/data/nostoc_anabaena_co-culture/hifi_meta/sequences-hifiasm_meta_assembly")
input_raw = [
    ((data/"Ana_PS.fna"), "sequences::assembly", dict()),
    ((data/"Nos_PS.fna"), "sequences::assembly", dict()),
    ((data/"SynC_PS.fna"), "sequences::assembly", dict()),
    ((data/"SynT_PS.fna"), "sequences::assembly", dict()),
]
_, _hash = KeyGenerator.FromStr("".join(str(p) for p, t, m in input_raw))
in_dir = base_dir/f"{notebook_name}/inputs.{_hash}.xgdb"
todo = {}
for p, t, m in input_raw:
    asm = p
    todo[p] = {asm}

lib_path = Path("/home/tony/workspace/tools/MetasmithLibraries")
d_path = lib_path/"data_types"
if in_dir.exists():
    inputs = DataInstanceLibrary.Load(in_dir)
else:
    inputs = DataInstanceLibrary(in_dir)
    inputs.Purge()
    inputs.AddTypeLibrary("sequences", DataTypeLibrary.Load(d_path/"sequences.yml"))
    inputs.AddTypeLibrary("ncbi", DataTypeLibrary.Load(d_path/"ncbi.yml"))
    for p, t, m in input_raw:
        reads = inputs.AddItem(p, t)
    inputs.Save()

# inputs = DataInstanceLibrary.Load(in_dir)



ref_path = base_dir/f"{host}_ref_dbs"
loaded = False
try:
    ref_dbs = DataInstanceLibrary.Load(ref_path)
    loaded = True
except:
    pass
if not loaded:
    match(host):
        case "sockeye":
            remote_lib = Path("/arc/project/st-shallam-1/pwy_group/lib")
        case _:
            assert False, f"please add remote lib location for [{host}]"
    ref_dbs = DataInstanceLibrary(ref_path)
    ref_dbs.Purge()
    ref_dbs.AddTypeLibrary("taxonomy", DataTypeLibrary.Load(d_path/"taxonomy.yml"))
    ref_dbs.AddItem(remote_lib/"metabuli/gtdb", "taxonomy::metabuli_ref")
    ref_dbs.Save()

resources = [
    DataInstanceLibrary.Load(lib_path/f"resources/{n}")
    for n in [
        "containers",
        # "lib",
    ]
]+[
    ref_dbs,
]

transforms = [
    TransformInstanceLibrary.Load(lib_path/f"transforms/{n}")
    for n in [
        # "logistics",
        # "assembly",
        "metagenomics",
    ]
]

# lib = TransformInstanceLibrary.Load(lib_path/f"transforms/assembly")
# k = Path("flye.py")
# if k in lib.manifest:
#     del lib.manifest[k]
# transforms.append(lib)

targets = TargetBuilder()
for n, p in [
        # ("taxonomy::metabuli_ref",              set()),
        # ("containers::metabuli.oci",              set()),
        ("taxonomy::metabuli",              set()),
        ("taxonomy::metabuli_krona",        set()),
        ("taxonomy::metabuli_report",       set()),
        # ("taxonomy::checkm_stats",          set()),
    ]:
    targets.Add(n, p)

task = smith.GenerateWorkflow(
    samples=[inputs.AsView(mask=v) for k, v in todo.items()],
    resources=resources,
    transforms=transforms,
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

# smith.StageWorkflow(task, on_exist="update_all", verify_external_paths=True)
smith.StageWorkflow(task, on_exist="clear", verify_external_paths=False)

params = dict(
    slurmAccount=SLURM_ACCOUNT,
    # executor=dict(
    #     queueSize=500,
    # ),
    process=dict(
        array=4,
        tries=1,
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
