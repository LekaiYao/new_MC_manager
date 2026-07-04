import os
import re
import subprocess
import sys
CYAN = "\033[96m"
ORANGE = "\033[38;5;214m"
RED = "\033[91m"
GREEN = "\033[92m"
RESET = "\033[0m"

allowed_steps = {"GEN", "SIM", "DIGI", "HLT", "RECO", "MINIAOD"}

if len(sys.argv) < 2:
    print("Usage: python3 CrabTask_large_submission_handler.py <finished STEP>")
    print("Allowed STEP values: GEN, SIM, DIGI, HLT, RECO, MINIAOD")
    sys.exit(1)

current_step = sys.argv[1].upper()
if current_step not in allowed_steps:
    print(f"[X] Invalid STEP: {current_step}")
    print("Allowed STEP values: GEN, SIM, DIGI, HLT, RECO, MINIAOD")
    sys.exit(1)

# Crab configuration file
STEP_DIR_MAP = {
    "SIM": "../CMSSW_10_6_17_patch1/src",
    "DIGI": "../CMSSW_10_6_17_patch1/src",
    "HLT": "../CMSSW_10_2_16_UL/src",
    "RECO": "../CMSSW_10_6_17_patch1/src",
    "MINIAOD": "../CMSSW_10_6_17_patch1/src",
    "NTUPLE":"../CMSSW_10_6_20/src/NtupleMaker/NtupleMaker/test",
}

NEXT_STEP_MAP = {
    "GEN": "SIM",
    "SIM": "DIGI",
    "DIGI": "HLT",
    "HLT": "RECO",
    "RECO": "MINIAOD",
    "MINIAOD": "NTUPLE",
}

# need double check the values for each step
STEP_RESOURCE_MAP = {
    "SIM":     {"maxMemoryMB": 2000, "numCores": 1},
    "DIGI":    {"maxMemoryMB": 2000, "numCores": 1},
    "HLT":     {"maxMemoryMB": 2000, "numCores": 1},
    "RECO":    {"maxMemoryMB": 2000, "numCores": 1},
    "MINIAOD": {"maxMemoryMB": 2000, "numCores": 1},
    "NTUPLE":  {"maxMemoryMB": 2000, "numCores": 1},
}

current_step = sys.argv[1].upper()
next_step = NEXT_STEP_MAP[current_step]
work_dir = STEP_DIR_MAP[next_step]
crab_config_file = os.path.join(work_dir, "crab3_Config.py")
step_resources = STEP_RESOURCE_MAP[next_step]
max_memory_mb = step_resources["maxMemoryMB"]
num_cores = step_resources["numCores"]

# Paths to log files and CRAB configuration file
DIR_file = f"./txt/CrabTask_manager_OUTPUT_DIRs_{current_step}.txt"

submission_log_file = f"./log/CrabTask_large_submission_{next_step}.log"
os.makedirs("./log", exist_ok=True)

#####################################################################################
## Toggle submission (set to False for testing, True for actual submission) ##
##                                                                          ## 
#######################                                                     ##
submit_jobs = False   ##                                                     ##
#######################                                                     ##
##                                                                          ##
## Toggle submission (set to False for testing, True for actual submission) ##
#####################################################################################

# Open and read DIRs from the log file
try:
    with open(DIR_file, "r") as log_file:
        datasets = log_file.readlines()
except FileNotFoundError:
    print(f"{RED}[X] Error: Log file {DIR_file} not found. {RESET}")
    exit(1)

# Open the submission log file
with open(submission_log_file, "w") as log_file:
    log_file.write("="*60 + "\n")
    log_file.write("CRAB Submission Log\n")
    log_file.write("="*60 + "\n\n")

    last_step_message = None

    for dataset in datasets:
        dataset = dataset.strip()  # Remove whitespace/newline
        if not dataset:
            continue  

        print(f"\n[..] Processing dataset: {dataset}")
        log_file.write(f"\n[..] Processing dataset: {dataset}\n")

        # Extract the dataset name
        match = re.search(r'-(.*)-', dataset)
        if not match:
            print(f"{RED}[X] Error: Could not extract dataset name. {RESET}")
            log_file.write(f"[X] Error: Could not extract dataset name.\n")
            continue

        dataset_name = match.group(1)

        # Determine the next processing step and corresponding PSet
        new_dataset_name = dataset_name.replace(current_step, next_step)
        pset_file = f"BPH_{next_step}_13TeV_cfg.py"
        last_step_message = f"Starting step: {next_step}"

        # Update CRAB config with new dataset and PSet
        try:
            with open(crab_config_file, "r") as file:
                config_content = file.readlines()

            new_content = []
            for line in config_content:
                if "config.Data.inputDataset" in line:
                    new_content.append(f"config.Data.inputDataset = '{dataset}'\n")
                elif "config.General.requestName" in line:
                    new_content.append(f"config.General.requestName = '{new_dataset_name}'\n")
                elif "config.Data.outputDatasetTag" in line:
                    new_content.append(f"config.Data.outputDatasetTag = '{new_dataset_name}'\n")
                elif "config.JobType.psetName" in line and next_step != "NTUPLE":
                    new_content.append(f"config.JobType.psetName = '{pset_file}'\n")  # Automatically set PSet
                elif "config.JobType.maxMemoryMB" in line:
                    new_content.append(f"config.JobType.maxMemoryMB = {max_memory_mb}\n")
                elif "config.JobType.numCores" in line:
                    new_content.append(f"config.JobType.numCores = {num_cores}\n")
                else:
                    new_content.append(line)

            with open(crab_config_file, "w") as file:
                file.writelines(new_content)

            print(f"{GREEN}[OK] CRAB config updated.{RESET}")

            # Submit CRAB job only if submit_jobs is True
            if submit_jobs:
                submit_command = f"cd {work_dir} && crab submit -c crab3_Config.py"
                result = subprocess.run(submit_command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)

                # Log the output
                log_file.write("\n--- CRAB Submission Output ---\n")
                log_file.write(result.stdout)
                log_file.write("--- End of Submission Output ---\n\n")

                if result.returncode == 0:
                    print(f"{CYAN} [->] Successfully submitted CRAB job! {RESET}")
                else:
                    if "Please change the requestName in the config file" in result.stdout:
                        print(f"{ORANGE}[!] Task previously submitted -- skipping it.{RESET}")
                    else:
                        print(f"{RED}[X] Submission failed! Check the log file for details. {RESET}")
            else:
                print(f"{ORANGE}[!] SAFE MODE: CRAB job submission skipped.{RESET}")
                log_file.write(f"[!] SAFE MODE: CRAB job submission skipped.\n")

        except Exception as e:
            print(f"{RED} [X] Error modifying {crab_config_file}: {e} {RESET}")
            log_file.write(f"[X] Error modifying {crab_config_file}: {e}\n")

# Print and log the last step message once at the end
if last_step_message:
    step_summary = f"\n{'='*30}\n {last_step_message} \n{'='*30}\n"
    print(step_summary)
    with open(submission_log_file, "a") as log_file:
        log_file.write(step_summary)

print(f"\n Submission log saved to: {submission_log_file}")


