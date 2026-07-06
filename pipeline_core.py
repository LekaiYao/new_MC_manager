import datetime
import json
import os
import re
import shlex
import subprocess
import tempfile
MANAGER_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_FILE = os.path.join(MANAGER_DIR, 'config.json')
LOCAL_EXCLUSIONS_FILE = os.path.join(MANAGER_DIR, '.local', 'exclusions.json')
STATE_DIR = os.path.join(MANAGER_DIR, 'state')
LOG_DIR = os.path.join(MANAGER_DIR, 'log')
TXT_DIR = os.path.join(MANAGER_DIR, 'txt')
STATE_FILE = os.path.join(STATE_DIR, 'pipeline_state.json')
EVENTS_FILE = os.path.join(LOG_DIR, 'pipeline_events.jsonl')
TABLE_FILE = os.path.join(STATE_DIR, 'pipeline_table.md')
BLUE = '\033[94m'
GREEN = '\033[32m'
ORANGE = '\033[38;5;214m'
BROWN = '\033[38;5;130m'
RED = '\033[91m'
GRAY = '\033[90m'
RESET = '\033[0m'
_PROXY_READY = False
def ensure_runtime_dirs():
    for path in (STATE_DIR, LOG_DIR, TXT_DIR):
        if not os.path.isdir(path):
            os.makedirs(path)
def utc_timestamp():
    return datetime.datetime.utcnow().isoformat() + 'Z'
def load_config():
    with open(CONFIG_FILE, 'r') as handle:
        config = json.load(handle)
    config['allowed_steps'] = set(config['steps'])
    return config
def load_exclusions():
    payload = {}
    if os.path.exists(LOCAL_EXCLUSIONS_FILE):
        with open(LOCAL_EXCLUSIONS_FILE, 'r') as handle:
            payload = json.load(handle)
    return {
        'excluded_lineages': set(payload.get('excluded_lineages', [])),
        'excluded_samples': set(payload.get('excluded_samples', [])),
        'excluded_request_names': set(payload.get('excluded_request_names', [])),
        'excluded_crab_projects': set(payload.get('excluded_crab_projects', [])),
    }

def sample_matches_exclusions(sample_id, metadata, exclusions):
    if sample_id in exclusions['excluded_samples']:
        return True
    if metadata.get('request_name') in exclusions['excluded_request_names']:
        return True
    crab_project_name = metadata.get('crab_project_name')
    if crab_project_name in exclusions['excluded_crab_projects']:
        return True
    return False

def lineage_matches_exclusions(lineage, exclusions):
    if lineage.get('lineage_id') in exclusions['excluded_lineages']:
        return True
    for sample_id in lineage.get('sample_ids', []):
        if sample_id in exclusions['excluded_samples']:
            return True
    for step_info in lineage.get('steps', {}).values():
        if step_info.get('request_name') in exclusions['excluded_request_names']:
            return True
        if step_info.get('crab_project_name') in exclusions['excluded_crab_projects']:
            return True
    return False
def step_index(step, config):
    return config['steps'].index(step)
def get_next_step(step, config):
    idx = step_index(step, config)
    if idx + 1 >= len(config['steps']):
        return None
    return config['steps'][idx + 1]
def get_active_steps(config):
    terminal_idx = step_index(config['terminal_step'], config)
    return config['steps'][: terminal_idx + 1]
def get_step_dir(step, config):
    return os.path.abspath(os.path.join(MANAGER_DIR, config['step_dirs'][step]))
def get_crab_projects_dir(step, config):
    return os.path.abspath(os.path.join(MANAGER_DIR, config['crab_projects_dirs'][step]))
def parse_jobs_count(raw_value):
    if raw_value is None:
        return 0
    value = raw_value.strip()
    if '/' in value:
        value = value.split('/')[0]
    match = re.search(r'(\d+)', value)
    return int(match.group(1)) if match else 0
def strip_ansi(text):
    return re.sub(r'\[[0-9;]*m', '', text)
def _safe_int(value):
    try:
        return int(value)
    except (TypeError, ValueError):
        return None
def _safe_float(value):
    try:
        return float(value)
    except (TypeError, ValueError):
        return None
def _extract_processed_dataset(output_dataset):
    parts = output_dataset.strip().split('/')
    return parts[2] if len(parts) > 2 else ''
def infer_sample_id(step, crab_project_name, output_dataset):
    prefix = 'crab_MC2018_{0}_'.format(step)
    if crab_project_name.startswith(prefix):
        suffix = crab_project_name[len(prefix):]
        if suffix:
            return suffix
    processed = _extract_processed_dataset(output_dataset)
    if processed:
        match = re.search(r'{0}_(.*?)-[0-9a-f]{{8,}}$'.format(step), processed)
        if match:
            return match.group(1)
        return processed
    return crab_project_name.replace('crab_', '', 1)
def extract_metadata_from_crab_log(job_dir, step):
    crab_log = os.path.join(job_dir, 'crab.log')
    metadata = {
        'request_name': None,
        'output_dataset_tag': None,
        'input_dataset': None,
        'sample_id': None,
    }
    if not os.path.exists(crab_log):
        return metadata
    with open(crab_log, 'r') as handle:
        content = handle.read()
    request_match = re.search(r"config\.General\.requestName = '([^']+)'", content)
    tag_match = re.search(r"config\.Data\.outputDatasetTag = '([^']+)'", content)
    input_match = re.search(r"config\.Data\.inputDataset = '([^']+)'", content)
    if request_match:
        metadata['request_name'] = request_match.group(1)
    if input_match:
        metadata['input_dataset'] = input_match.group(1)
    if tag_match:
        metadata['output_dataset_tag'] = tag_match.group(1)
        sample_match = re.search(r'MC2018_{0}_(.*)$'.format(step), metadata['output_dataset_tag'])
        if sample_match:
            metadata['sample_id'] = sample_match.group(1)
    return metadata
def get_crab_job_dirs(step, config):
    crab_jobs_dir = get_crab_projects_dir(step, config)
    if not os.path.isdir(crab_jobs_dir):
        return []
    entries = []
    for entry in os.listdir(crab_jobs_dir):
        full_path = os.path.join(crab_jobs_dir, entry)
        if os.path.isdir(full_path) and step in entry:
            entries.append(full_path)
    return sorted(entries)
def run_cmssw_script(step, script_body, config, env=None):
    step_dir = get_step_dir(step, config)
    with tempfile.NamedTemporaryFile(
        mode='w',
        suffix='.sh',
        prefix='new_mc_manager_',
        delete=False,
    ) as handle:
        script_path = handle.name
        handle.write('#!/bin/bash\n')
        handle.write('set -e\n')
        handle.write('cd {0}\n'.format(shlex.quote(step_dir)))
        handle.write('eval "$(scramv1 runtime -sh)" >/dev/null 2>&1\n')
        handle.write(script_body)
        if not script_body.endswith('\n'):
            handle.write('\n')
    os.chmod(script_path, 0o700)
    try:
        cmd = ['cmssw-el7', '--command-to-run', 'bash {0}'.format(shlex.quote(script_path))]
        return subprocess.run(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            universal_newlines=True,
            env=env,
        )
    finally:
        if os.path.exists(script_path):
            os.unlink(script_path)
def ensure_proxy(step, config):
    global _PROXY_READY
    if _PROXY_READY:
        return subprocess.CompletedProcess(args=['proxy-cache'], returncode=0, stdout='cached', stderr='')
    script_body = (
        'if ! voms-proxy-info -exists -valid 12:00 >/dev/null 2>&1; then\n'
        '  voms-proxy-init --rfc --voms cms --valid 192:00 >/dev/null\n'
        'fi\n'
        'voms-proxy-info -timeleft\n'
    )
    result = run_cmssw_script(step, script_body, config)
    if result.returncode == 0:
        _PROXY_READY = True
    return result
def run_in_cmssw_env(step, command, config, require_proxy=True):
    if require_proxy:
        proxy_result = ensure_proxy(step, config)
        if proxy_result.returncode != 0:
            return proxy_result
    return run_cmssw_script(step, command, config)
def run_crab_status(step, job_dir, config, long_format=False):
    command = 'crab status {0}-d {1}'.format('--long ' if long_format else '', shlex.quote(job_dir))
    return run_in_cmssw_env(step, command, config, require_proxy=True)
def parse_crab_status_output(stdout):
    clean = strip_ansi(stdout)
    lines = clean.splitlines()
    record = {
        'task_name': None,
        'crab_server_status': None,
        'scheduler_status': None,
        'dashboard_url': None,
        'output_dataset': None,
        'warnings': [],
        'job_counts': {
            'finished': 0,
            'running': 0,
            'idle': 0,
            'unsubmitted': 0,
            'toRetry': 0,
            'failed': 0,
            'transferring': 0,
        },
        'resource_summary': {
            'memory_mb': {'min': None, 'max': None, 'avg': None},
            'runtime': {'min': None, 'max': None, 'avg': None},
            'cpu_efficiency_pct': {'min': None, 'max': None, 'avg': None},
            'waste': {'value': None, 'fraction_pct': None},
        },
        'publication': {
            'available': False,
            'done': 0,
            'done_pct': None,
        },
    }
    counts_pattern = re.compile(
        r'(idle|running|toRetry|unsubmitted|finished|failed|transferring)\s+([0-9.]+)%\s+\(([^)]+)\)'
    )
    publication_pattern = re.compile(r'done\s+([0-9.]+)%\s+\(([^)]+)\)')
    for line in lines:
        stripped = line.strip()
        if line.startswith('Task name:'):
            record['task_name'] = line.split('Task name:')[-1].strip()
        elif line.startswith('Status on the CRAB server:'):
            record['crab_server_status'] = line.split(':', 1)[-1].strip()
        elif line.startswith('Status on the scheduler:'):
            record['scheduler_status'] = line.split(':', 1)[-1].strip()
        elif line.startswith('Dashboard monitoring URL:'):
            record['dashboard_url'] = line.split(':', 1)[-1].strip()
        elif 'Output dataset:' in line:
            record['output_dataset'] = line.split('Output dataset:')[-1].strip()
        elif stripped.startswith('Warning:'):
            record['warnings'].append(stripped)
        elif 'No publication information available yet' in line:
            record['publication']['available'] = False
        if 'Publication status of' in line or record['publication']['available']:
            publication_match = publication_pattern.search(line)
            if publication_match:
                pct, raw_count = publication_match.groups()
                record['publication']['available'] = True
                record['publication']['done_pct'] = _safe_float(pct)
                record['publication']['done'] = parse_jobs_count(raw_count)
        match = counts_pattern.search(line)
        if match:
            state, pct, raw_count = match.groups()
            record['job_counts'][state] = parse_jobs_count(raw_count)
            record['job_counts'][state + '_pct'] = _safe_float(pct)
        if stripped.startswith('* Memory:'):
            match = re.search(r'Memory:\s*(\d+)MB min,\s*(\d+)MB max,\s*(\d+)MB ave', stripped)
            if match:
                record['resource_summary']['memory_mb'] = {
                    'min': _safe_int(match.group(1)),
                    'max': _safe_int(match.group(2)),
                    'avg': _safe_int(match.group(3)),
                }
        elif stripped.startswith('* Runtime:'):
            match = re.search(r'Runtime:\s*(\S+) min,\s*(\S+) max,\s*(\S+) ave', stripped)
            if match:
                record['resource_summary']['runtime'] = {
                    'min': match.group(1),
                    'max': match.group(2),
                    'avg': match.group(3),
                }
        elif stripped.startswith('* CPU eff:'):
            match = re.search(r'CPU eff:\s*(\d+)% min,\s*(\d+)% max,\s*(\d+)% ave', stripped)
            if match:
                record['resource_summary']['cpu_efficiency_pct'] = {
                    'min': _safe_int(match.group(1)),
                    'max': _safe_int(match.group(2)),
                    'avg': _safe_int(match.group(3)),
                }
        elif stripped.startswith('* Waste:'):
            match = re.search(r'Waste:\s*(\S+)\s*\((\d+)% of total\)', stripped)
            if match:
                record['resource_summary']['waste'] = {
                    'value': match.group(1),
                    'fraction_pct': _safe_int(match.group(2)),
                }
    return record
def classify_flags(record):
    flags = []
    retry_pct = record['job_counts'].get('toRetry_pct') or 0.0
    cpu_avg = record['resource_summary']['cpu_efficiency_pct'].get('avg')
    waste_pct = record['resource_summary']['waste'].get('fraction_pct')
    if retry_pct >= 10.0:
        flags.append('high_retry')
    if cpu_avg is not None and cpu_avg < 10:
        flags.append('low_cpu_efficiency')
    if waste_pct is not None and waste_pct >= 50:
        flags.append('high_waste')
    if record.get('output_dataset'):
        flags.append('has_output_dataset')
    return flags
def collect_step_status(step, write_legacy_logs=True, config=None):
    ensure_runtime_dirs()
    if config is None:
        config = load_config()
    if step not in config['allowed_steps']:
        raise ValueError('Invalid step: {0}'.format(step))
    job_dirs = get_crab_job_dirs(step, config)
    step_payload = {
        'step': step,
        'generated_at': utc_timestamp(),
        'crab_projects_dir': get_crab_projects_dir(step, config),
        'records': [],
        'summary': {
            'projects_total': len(job_dirs),
            'projects_with_output_dataset': 0,
            'jobs_finished': 0,
            'jobs_running': 0,
            'jobs_failed': 0,
            'jobs_retry': 0,
            'jobs_idle': 0,
            'jobs_unsubmitted': 0,
            'jobs_transferring': 0,
            'flags': [],
        },
    }
    legacy_log_lines = [
        '=' * 60,
        'CRAB Status Log - {0}'.format(datetime.datetime.now()),
        'Processing jobs matching: {0}'.format(step),
        '=' * 60,
    ]
    output_datasets = []
    for job_dir in job_dirs:
        result = run_crab_status(step, job_dir, config)
        legacy_log_lines.append('')
        legacy_log_lines.append('Checking status of {0}...'.format(job_dir))
        metadata = extract_metadata_from_crab_log(job_dir, step)
        fallback_sample_id = metadata['sample_id'] or os.path.basename(job_dir).replace('crab_', '', 1)
        if result.returncode != 0:
            step_payload['records'].append({
                'step': step,
                'crab_project_dir': job_dir,
                'crab_project_name': os.path.basename(job_dir),
                'sample_id': fallback_sample_id,
                'status': 'command_failed',
                'command': 'crab status',
                'return_code': result.returncode,
                'stderr': result.stderr.strip(),
                'generated_at': step_payload['generated_at'],
                'flags': ['status_command_failed'],
                'request_name': metadata['request_name'],
                'input_dataset': metadata['input_dataset'],
            })
            legacy_log_lines.append(result.stderr.strip())
            legacy_log_lines.append('#' * 40)
            legacy_log_lines.append('#' * 40)
            continue
        parsed = parse_crab_status_output(result.stdout)
        output_dataset = parsed['output_dataset']
        if output_dataset:
            output_datasets.append(output_dataset)
        record = {
            'step': step,
            'generated_at': step_payload['generated_at'],
            'crab_project_dir': job_dir,
            'crab_project_name': os.path.basename(job_dir),
            'sample_id': metadata['sample_id'] or infer_sample_id(step, os.path.basename(job_dir), output_dataset or ''),
            'status': parsed['scheduler_status'] or parsed['crab_server_status'] or 'UNKNOWN',
            'task_name': parsed['task_name'],
            'dashboard_url': parsed['dashboard_url'],
            'output_dataset': output_dataset,
            'warnings': parsed['warnings'],
            'job_counts': parsed['job_counts'],
            'resource_summary': parsed['resource_summary'],
            'flags': [],
            'request_name': metadata['request_name'],
            'output_dataset_tag': metadata['output_dataset_tag'],
            'input_dataset': metadata['input_dataset'],
        }
        record['flags'] = classify_flags(record)
        step_payload['records'].append(record)
        step_payload['summary']['jobs_finished'] += record['job_counts']['finished']
        step_payload['summary']['jobs_running'] += record['job_counts']['running']
        step_payload['summary']['jobs_failed'] += record['job_counts']['failed']
        step_payload['summary']['jobs_retry'] += record['job_counts']['toRetry']
        step_payload['summary']['jobs_idle'] += record['job_counts']['idle']
        step_payload['summary']['jobs_unsubmitted'] += record['job_counts']['unsubmitted']
        step_payload['summary']['jobs_transferring'] += record['job_counts']['transferring']
        if output_dataset:
            step_payload['summary']['projects_with_output_dataset'] += 1
        legacy_log_lines.append(result.stdout.rstrip())
        legacy_log_lines.append('#' * 40)
        legacy_log_lines.append('#' * 40)
    aggregated_flags = set()
    for record in step_payload['records']:
        aggregated_flags.update(record.get('flags', []))
    step_payload['summary']['flags'] = sorted(aggregated_flags)
    output_datasets = sorted(set(output_datasets))
    if write_legacy_logs:
        with open(os.path.join(LOG_DIR, 'CrabTask_manager_jobStatus_{0}.log'.format(step)), 'w') as handle:
            handle.write('\n'.join(legacy_log_lines) + '\n')
        with open(os.path.join(TXT_DIR, 'CrabTask_manager_OUTPUT_DIRs_{0}.txt'.format(step)), 'w') as handle:
            for dataset in output_datasets:
                handle.write(dataset + '\n')
    return step_payload
def build_default_state(config):
    return {
        'generated_at': None,
        'terminal_step': config['terminal_step'],
        'samples': {},
    }
def load_pipeline_state(config=None):
    if config is None:
        config = load_config()
    if not os.path.exists(STATE_FILE):
        return build_default_state(config)
    with open(STATE_FILE, 'r') as handle:
        state = json.load(handle)
    state.setdefault('samples', {})
    state['terminal_step'] = config['terminal_step']
    return state
def save_pipeline_state(state):
    ensure_runtime_dirs()
    state['generated_at'] = utc_timestamp()
    with open(STATE_FILE, 'w') as handle:
        json.dump(state, handle, indent=2, sort_keys=True)
        handle.write('\n')
    return state
def append_event(event_type, payload):
    ensure_runtime_dirs()
    event = {
        'timestamp': utc_timestamp(),
        'event_type': event_type,
        'payload': payload,
    }
    with open(EVENTS_FILE, 'a') as handle:
        handle.write(json.dumps(event, sort_keys=True) + '\n')
def ensure_sample_state(sample_id, config):
    return {
        'sample_id': sample_id,
        'workflow_complete': False,
        'current_step': None,
        'current_status': 'DISCOVERED',
        'ready_for_next_step': False,
        'next_step': None,
        'steps': {},
    }
def merge_discovered_samples(state, config, exclusions=None):
    if exclusions is None:
        exclusions = load_exclusions()
    active_steps = get_active_steps(config)
    for step in active_steps:
        for job_dir in get_crab_job_dirs(step, config):
            metadata = extract_metadata_from_crab_log(job_dir, step)
            metadata['crab_project_name'] = os.path.basename(job_dir)
            sample_id = metadata['sample_id']
            if not sample_id:
                sample_id = infer_sample_id(step, os.path.basename(job_dir), '')
            if sample_matches_exclusions(sample_id, metadata, exclusions):
                continue
            sample = state['samples'].setdefault(sample_id, ensure_sample_state(sample_id, config))
            step_info = sample['steps'].setdefault(step, {})
            step_info.setdefault('step', step)
            step_info['crab_project_dir'] = job_dir
            step_info['crab_project_name'] = os.path.basename(job_dir)
            if metadata['request_name']:
                step_info['request_name'] = metadata['request_name']
            if metadata['output_dataset_tag']:
                step_info['output_dataset_tag'] = metadata['output_dataset_tag']
            if metadata['input_dataset']:
                step_info['input_dataset'] = metadata['input_dataset']
            if sample['current_step'] is None or step_index(step, config) > step_index(sample['current_step'], config):
                sample['current_step'] = step
                sample['next_step'] = get_next_step(step, config)
                if not sample.get('workflow_complete'):
                    sample['current_status'] = sample.get('current_status') or 'DISCOVERED'
    return state
def cached_step_completed(step_info, config):
    finished_pct = step_info.get('finished_pct')
    publication_done_pct = step_info.get('publication_done_pct')
    if finished_pct is None or publication_done_pct is None:
        return False
    threshold = float(config['finished_threshold_pct'])
    return finished_pct > threshold and publication_done_pct > threshold and abs(finished_pct - publication_done_pct) <= 0.1

def step_completed(record, config):
    finished_pct = record['job_counts'].get('finished_pct') or 0.0
    publication_done_pct = record.get('publication', {}).get('done_pct')
    if publication_done_pct is None:
        return False
    threshold = float(config['finished_threshold_pct'])
    pct_gap = abs(finished_pct - publication_done_pct)
    return finished_pct > threshold and publication_done_pct > threshold and pct_gap <= 0.1
def update_sample_from_status(sample, step, record, config):
    step_info = sample['steps'].setdefault(step, {})
    step_info.update({
        'step': step,
        'request_name': record.get('request_name'),
        'task_name': record.get('task_name'),
        'crab_project_dir': record.get('crab_project_dir'),
        'crab_project_name': record.get('crab_project_name'),
        'status': record.get('status'),
        'dashboard_url': record.get('dashboard_url'),
        'warnings': record.get('warnings', []),
        'job_counts': record.get('job_counts', {}),
        'resource_summary': record.get('resource_summary', {}),
        'output_dataset': record.get('output_dataset'),
        'input_dataset': record.get('input_dataset') or step_info.get('input_dataset'),
        'output_dataset_tag': record.get('output_dataset_tag'),
        'finished_pct': record.get('job_counts', {}).get('finished_pct'),
        'transferring': record.get('job_counts', {}).get('transferring'),
        'publication': record.get('publication', {}),
        'publication_done': record.get('publication', {}).get('done'),
        'publication_done_pct': record.get('publication', {}).get('done_pct'),
        'completed': False,
        'checked_at': utc_timestamp(),
    })
    sample['current_step'] = step
    sample['current_status'] = record.get('status')
    sample['ready_for_next_step'] = False
    sample['next_step'] = get_next_step(step, config)
    if step_completed(record, config):
        step_info['completed'] = True
        if step == config['terminal_step']:
            sample['workflow_complete'] = True
            sample['current_status'] = 'WORKFLOW_COMPLETE'
            sample['ready_for_next_step'] = False
            sample['next_step'] = None
        else:
            sample['current_status'] = 'READY_FOR_NEXT_STEP' if step_info.get('output_dataset') else 'COMPLETED_NO_OUTPUT_DATASET'
            sample['ready_for_next_step'] = bool(step_info.get('output_dataset'))
    else:
        sample['workflow_complete'] = False
    return sample
def check_active_samples(config=None):
    ensure_runtime_dirs()
    if config is None:
        config = load_config()
    exclusions = load_exclusions()
    state = load_pipeline_state(config)
    state = merge_discovered_samples(state, config, exclusions=exclusions)
    checked = []
    skipped_ready = []
    skipped_complete = []
    skipped_excluded = []
    all_lineages = build_lineage_view(state, config, exclusions=None)
    for lineage in all_lineages:
        if lineage_matches_exclusions(lineage, exclusions):
            skipped_excluded.append(lineage['lineage_id'])
            continue
        lineage_id = lineage['lineage_id']
        sample_id = lineage.get('latest_sample_id')
        sample = lineage.get('latest_sample')
        if not sample_id or sample is None:
            continue
        if sample.get('workflow_complete'):
            skipped_complete.append(lineage_id)
            continue
        current_step = sample.get('current_step')
        if sample.get('ready_for_next_step'):
            cached_step = sample.get('steps', {}).get(current_step, {}) if current_step else {}
            if cached_step_completed(cached_step, config):
                skipped_ready.append(lineage_id)
                continue
            sample['ready_for_next_step'] = False
            if sample.get('current_status') == 'READY_FOR_NEXT_STEP':
                sample['current_status'] = cached_step.get('status')
        current_step = sample.get('current_step')
        if not current_step:
            continue
        step_info = sample.get('steps', {}).get(current_step, {})
        job_dir = step_info.get('crab_project_dir')
        if not job_dir:
            continue
        result = run_crab_status(current_step, job_dir, config)
        if result.returncode != 0:
            sample['current_status'] = 'command_failed'
            sample['last_error'] = result.stderr.strip()
            checked.append({
                'lineage_id': lineage_id,
                'sample_id': sample_id,
                'step': current_step,
                'status': sample['current_status'],
            })
            continue
        record = parse_crab_status_output(result.stdout)
        record['request_name'] = step_info.get('request_name')
        record['crab_project_dir'] = job_dir
        record['crab_project_name'] = step_info.get('crab_project_name')
        record['output_dataset_tag'] = step_info.get('output_dataset_tag')
        update_sample_from_status(sample, current_step, record, config)
        finished_pct = record['job_counts'].get('finished_pct') or 0.0
        failed_pct = record['job_counts'].get('failed_pct') or 0.0
        publication_done_pct = record.get('publication', {}).get('done_pct')
        publication_for_threshold = publication_done_pct or 0.0
        checked.append({
            'lineage_id': lineage_id,
            'sample_id': sample_id,
            'step': current_step,
            'status': sample['current_status'],
            'finished_pct': record['job_counts'].get('finished_pct'),
            'running_pct': record['job_counts'].get('running_pct'),
            'idle_pct': record['job_counts'].get('idle_pct'),
            'failed_pct': record['job_counts'].get('failed_pct'),
            'transferring': record['job_counts'].get('transferring'),
            'publication_done_pct': publication_done_pct,
            'ready_for_next_step': sample['ready_for_next_step'],
            'workflow_complete': sample['workflow_complete'],
            'resubmit_candidate': (not sample['ready_for_next_step']) and (not sample['workflow_complete']) and (publication_for_threshold + failed_pct > float(config['finished_threshold_pct'])),
        })
    save_pipeline_state(state)
    append_event('check-active', {
        'checked_samples': checked,
        'skipped_ready': skipped_ready,
        'skipped_complete': skipped_complete,
        'terminal_step': config['terminal_step'],
        'mode': 'lineage-latest',
        'skipped_excluded': skipped_excluded,
    })
    return state, checked, skipped_ready, skipped_complete, skipped_excluded
def sample_version(sample_id):
    match = re.search(r'_v(\d+)_', sample_id)
    return int(match.group(1)) if match else 0

def get_previous_step(step, config):
    idx = step_index(step, config)
    if idx <= 0:
        return None
    return config['steps'][idx - 1]

def build_output_dataset_index(state):
    output_index = {}
    for sample_id, sample in state.get('samples', {}).items():
        for step, step_info in sample.get('steps', {}).items():
            dataset = step_info.get('output_dataset')
            if dataset:
                output_index[(step, dataset)] = sample_id
    return output_index

def build_sample_parent_map(state, config=None):
    if config is None:
        config = load_config()
    output_index = build_output_dataset_index(state)
    parent_map = {}
    for sample_id, sample in state.get('samples', {}).items():
        parent_id = None
        for step in config['steps']:
            step_info = sample.get('steps', {}).get(step)
            if not step_info:
                continue
            previous_step = get_previous_step(step, config)
            input_dataset = step_info.get('input_dataset')
            if not previous_step or not input_dataset:
                continue
            candidate_parent = output_index.get((previous_step, input_dataset))
            if candidate_parent and candidate_parent != sample_id:
                parent_id = candidate_parent
                break
        parent_map[sample_id] = parent_id
    return parent_map

def resolve_lineage_root(sample_id, parent_map):
    seen = set()
    current = sample_id
    while parent_map.get(current) and current not in seen:
        seen.add(current)
        current = parent_map[current]
    return current

def build_lineage_path(latest_sample_id, parent_map):
    path = []
    seen = set()
    current = latest_sample_id
    while current and current not in seen:
        path.append(current)
        seen.add(current)
        current = parent_map.get(current)
    path.reverse()
    return path

def build_lineage_view(state, config=None, exclusions=None):
    if config is None:
        config = load_config()
    if exclusions is None:
        exclusions = load_exclusions()
    parent_map = build_sample_parent_map(state, config)
    lineages = {}
    for sample_id, sample in state.get('samples', {}).items():
        lineage_id = resolve_lineage_root(sample_id, parent_map)
        entry = lineages.setdefault(lineage_id, {
            'lineage_id': lineage_id,
            'sample_ids': [],
            'latest_sample_id': None,
            'latest_sample': None,
            'steps': {},
            'completed_steps': {},
            'ready_candidates': [],
            'path_sample_ids': [],
        })
        entry['sample_ids'].append(sample_id)
        latest_sample = entry['latest_sample']
        sample_step = sample.get('current_step')
        sample_step_idx = step_index(sample_step, config) if sample_step in config['allowed_steps'] else -1
        sample_key = (sample_step_idx, sample_version(sample_id), sample_id)
        if latest_sample is None:
            use_latest = True
        else:
            latest_id = entry['latest_sample_id']
            latest_step = latest_sample.get('current_step')
            latest_step_idx = step_index(latest_step, config) if latest_step in config['allowed_steps'] else -1
            latest_key = (latest_step_idx, sample_version(latest_id), latest_id)
            use_latest = sample_key > latest_key
        if use_latest:
            entry['latest_sample_id'] = sample_id
            entry['latest_sample'] = sample
    rendered = []
    for lineage_id in sorted(lineages.keys()):
        entry = lineages[lineage_id]
        latest_sample_id = entry['latest_sample_id']
        path_sample_ids = build_lineage_path(latest_sample_id, parent_map) if latest_sample_id else []
        entry['path_sample_ids'] = path_sample_ids
        entry['ready_candidates'] = []
        for sample_id in path_sample_ids:
            sample = state['samples'][sample_id]
            if sample.get('ready_for_next_step'):
                entry['ready_candidates'].append(sample_id)
            for step, step_info in sample.get('steps', {}).items():
                merged = dict(step_info)
                merged['sample_id'] = sample_id
                entry['steps'][step] = merged
                if step_info.get('completed') and step_info.get('output_dataset'):
                    entry['completed_steps'][step] = merged
        if lineage_matches_exclusions(entry, exclusions):
            continue
        rendered.append(entry)
    return rendered

def format_bool(value):
    return 'yes' if value else 'no'

def format_scalar(value):
    return '-' if value is None else str(value)

def colorize(text, color):
    return '{0}{1}{2}'.format(color, text, RESET)

def build_check_status_label(item):
    labels = []
    if item.get('workflow_complete'):
        labels.append(colorize('DONE', GREEN))
    elif item.get('ready_for_next_step'):
        labels.append(colorize('READY_NEXT', GREEN))
    if item.get('resubmit_candidate'):
        labels.append(colorize('READY_RESUBMIT', ORANGE))
    base_status = item.get('status')
    if not labels:
        labels.append(format_scalar(base_status))
    elif base_status not in (None, 'READY_FOR_NEXT_STEP', 'WORKFLOW_COMPLETE'):
        labels.insert(0, format_scalar(base_status))
    return '/'.join(labels)

def format_field_with_color(value, color=None):
    rendered = format_scalar(value)
    if color and rendered != '-':
        return colorize(rendered, color)
    return rendered

def format_lineage_status(lineage):
    latest_sample = lineage.get('latest_sample') or {}
    return {
        'current_step': latest_sample.get('current_step') or '',
        'current_status': latest_sample.get('current_status') or '',
        'ready_for_next_step': format_bool(latest_sample.get('ready_for_next_step')),
        'workflow_complete': format_bool(latest_sample.get('workflow_complete')),
        'latest_sample_id': lineage.get('latest_sample_id') or '',
    }

def build_next_request_name(next_step, sample_id):
    return 'MC2018_{0}_{1}'.format(next_step, sample_id)

def update_crab_config_file(submission):
    crab_config_file = submission['crab_config_file']
    if not os.path.exists(crab_config_file):
        raise RuntimeError('Missing CRAB config file: {0}'.format(crab_config_file))
    with open(crab_config_file, 'r') as handle:
        lines = handle.readlines()
    updated = []
    fields_seen = {
        'inputDataset': False,
        'requestName': False,
        'outputDatasetTag': False,
        'psetName': submission.get('pset_name') is None,
        'numCores': submission.get('num_cores') is None,
        'maxMemoryMB': submission.get('max_memory_mb') is None,
    }
    for line in lines:
        if 'config.Data.inputDataset' in line:
            updated.append("config.Data.inputDataset = '{0}'\n".format(submission['input_dataset']))
            fields_seen['inputDataset'] = True
        elif 'config.General.requestName' in line:
            updated.append("config.General.requestName = '{0}'\n".format(submission['request_name']))
            fields_seen['requestName'] = True
        elif 'config.Data.outputDatasetTag' in line:
            updated.append("config.Data.outputDatasetTag = '{0}'\n".format(submission['output_dataset_tag']))
            fields_seen['outputDatasetTag'] = True
        elif 'config.JobType.psetName' in line and submission.get('pset_name'):
            updated.append("config.JobType.psetName = '{0}'\n".format(submission['pset_name']))
            fields_seen['psetName'] = True
        else:
            updated.append(line)
    missing = [key for key, seen in fields_seen.items() if not seen]
    if missing:
        raise RuntimeError('Missing required config fields in {0}: {1}'.format(crab_config_file, ', '.join(missing)))
    with open(crab_config_file, 'w') as handle:
        handle.writelines(updated)

def register_submitted_step(state, submission, config, submit_result):
    sample_id = submission['next_sample_id']
    sample = state['samples'].setdefault(sample_id, ensure_sample_state(sample_id, config))
    step = submission['next_step']
    crab_project_name = 'crab_{0}'.format(submission['request_name'])
    step_info = sample['steps'].setdefault(step, {})
    step_info.update({
        'step': step,
        'request_name': submission['request_name'],
        'task_name': None,
        'crab_project_dir': os.path.join(get_crab_projects_dir(step, config), crab_project_name),
        'crab_project_name': crab_project_name,
        'status': 'SUBMITTED',
        'dashboard_url': None,
        'warnings': [],
        'job_counts': {},
        'resource_summary': {
            'memory_mb': {'min': None, 'max': None, 'avg': None},
            'runtime': {'min': None, 'max': None, 'avg': None},
            'cpu_efficiency_pct': {'min': None, 'max': None, 'avg': None},
            'waste': {'value': None, 'fraction_pct': None},
        },
        'output_dataset': None,
        'input_dataset': submission['input_dataset'],
        'output_dataset_tag': submission['output_dataset_tag'],
        'finished_pct': None,
        'transferring': 0,
        'completed': False,
        'checked_at': utc_timestamp(),
        'submit_stdout': submit_result.stdout.strip(),
        'submit_stderr': submit_result.stderr.strip(),
    })
    sample['current_step'] = step
    sample['current_status'] = 'SUBMITTED'
    sample['ready_for_next_step'] = False
    sample['workflow_complete'] = False
    sample['next_step'] = get_next_step(step, config)
    sample.pop('last_error', None)
    return sample

def execute_submit_plan(state, config=None, execute=False):
    if config is None:
        config = load_config()
    exclusions = load_exclusions()
    plan = build_submit_plan(state, config=config, exclusions=exclusions)
    plan['mode'] = 'execute' if execute else 'dry-run'
    plan['results'] = []
    if not execute:
        return state, plan
    for submission in plan.get('submissions', []):
        try:
            update_crab_config_file(submission)
        except Exception as exc:
            plan['results'].append({
                'lineage_id': submission['lineage_id'],
                'latest_sample_id': submission['latest_sample_id'],
                'next_step': submission['next_step'],
                'status': 'config_update_failed',
                'message': str(exc),
            })
            continue
        submit_result = run_in_cmssw_env(
            submission['next_step'],
            'crab submit -c {0}'.format(shlex.quote(os.path.basename(submission['crab_config_file']))),
            config,
            require_proxy=True,
        )
        if submit_result.returncode != 0:
            plan['results'].append({
                'lineage_id': submission['lineage_id'],
                'latest_sample_id': submission['latest_sample_id'],
                'next_step': submission['next_step'],
                'status': 'submit_failed',
                'return_code': submit_result.returncode,
                'stdout': submit_result.stdout.strip(),
                'stderr': submit_result.stderr.strip(),
            })
            continue
        register_submitted_step(state, submission, config, submit_result)
        plan['results'].append({
            'lineage_id': submission['lineage_id'],
            'latest_sample_id': submission['latest_sample_id'],
            'next_step': submission['next_step'],
            'status': 'submitted',
            'stdout': submit_result.stdout.strip(),
            'stderr': submit_result.stderr.strip(),
        })
    save_pipeline_state(state)
    return state, plan

def build_submit_plan(state, config=None, exclusions=None):
    if config is None:
        config = load_config()
    if exclusions is None:
        exclusions = load_exclusions()
    plan = {
        'generated_at': utc_timestamp(),
        'mode': 'dry-run',
        'submissions': [],
        'blocked': [],
    }
    for lineage in build_lineage_view(state, config, exclusions=exclusions):
        latest_sample = lineage.get('latest_sample') or {}
        latest_sample_id = lineage.get('latest_sample_id')
        current_step = latest_sample.get('current_step')
        current_status = latest_sample.get('current_status')
        if not latest_sample_id or not current_step:
            plan['blocked'].append({
                'lineage_id': lineage['lineage_id'],
                'reason': 'missing_current_step',
            })
            continue
        if latest_sample.get('workflow_complete'):
            plan['blocked'].append({
                'lineage_id': lineage['lineage_id'],
                'latest_sample_id': latest_sample_id,
                'current_step': current_step,
                'reason': 'workflow_complete',
            })
            continue
        if not latest_sample.get('ready_for_next_step'):
            plan['blocked'].append({
                'lineage_id': lineage['lineage_id'],
                'latest_sample_id': latest_sample_id,
                'current_step': current_step,
                'current_status': current_status,
                'reason': 'latest_step_not_ready',
                'ready_candidates': sorted(lineage.get('ready_candidates', [])),
            })
            continue
        next_step = get_next_step(current_step, config)
        step_info = latest_sample.get('steps', {}).get(current_step, {})
        input_dataset = step_info.get('output_dataset')
        if not next_step:
            plan['blocked'].append({
                'lineage_id': lineage['lineage_id'],
                'latest_sample_id': latest_sample_id,
                'current_step': current_step,
                'reason': 'no_next_step',
            })
            continue
        if not input_dataset:
            plan['blocked'].append({
                'lineage_id': lineage['lineage_id'],
                'latest_sample_id': latest_sample_id,
                'current_step': current_step,
                'reason': 'missing_output_dataset',
            })
            continue
        next_sample_id = latest_sample_id
        request_name = build_next_request_name(next_step, next_sample_id)
        existing_next = state.get('samples', {}).get(next_sample_id, {}).get('steps', {}).get(next_step)
        if existing_next and existing_next.get('crab_project_dir'):
            plan['blocked'].append({
                'lineage_id': lineage['lineage_id'],
                'latest_sample_id': latest_sample_id,
                'current_step': current_step,
                'reason': 'next_step_already_registered',
            })
            continue
        step_resources = config.get('step_resources', {}).get(next_step, {})
        plan['submissions'].append({
            'lineage_id': lineage['lineage_id'],
            'latest_sample_id': latest_sample_id,
            'next_sample_id': next_sample_id,
            'current_step': current_step,
            'next_step': next_step,
            'input_dataset': input_dataset,
            'request_name': request_name,
            'output_dataset_tag': request_name,
            'work_dir': get_step_dir(next_step, config),
            'crab_config_file': os.path.join(get_step_dir(next_step, config), 'crab3_Config.py'),
            'pset_name': 'BPH_{0}_13TeV_cfg.py'.format(next_step) if next_step != 'NTUPLE' else None,
            'num_cores': step_resources.get('numCores'),
            'max_memory_mb': step_resources.get('maxMemoryMB'),
        })
    return plan

def render_submit_plan(plan):
    lines = []
    lines.append('submit-next mode: {0}'.format(plan.get('mode', 'dry-run')))
    lines.append('planned submissions: {0}'.format(len(plan.get('submissions', []))))
    for item in plan.get('submissions', []):
        lines.append('  - {0}: {1} -> {2}'.format(item['lineage_id'], item['current_step'], item['next_step']))
        lines.append('    latest_sample_id={0}'.format(item['latest_sample_id']))
        lines.append('    next_sample_id={0}'.format(item['next_sample_id']))
        lines.append('    input_dataset={0}'.format(item['input_dataset']))
        lines.append('    requestName={0}'.format(item['request_name']))
        lines.append('    crab_config={0}'.format(item['crab_config_file']))
    if plan.get('results'):
        lines.append('execution results: {0}'.format(len(plan['results'])))
        for item in plan['results']:
            lines.append('  - {0}: next_step={1}, status={2}'.format(
                item.get('lineage_id'),
                item.get('next_step', ''),
                item.get('status'),
            ))
            if item.get('message'):
                lines.append('    message={0}'.format(item['message']))
            elif item.get('stderr'):
                lines.append('    stderr={0}'.format(item['stderr']))
    lines.append('blocked lineages: {0}'.format(len(plan.get('blocked', []))))
    for item in plan.get('blocked', []):
        lines.append('  - {0}: reason={1}, current_step={2}, latest_sample_id={3}, ready_candidates={4}'.format(
            item.get('lineage_id'),
            item.get('reason'),
            item.get('current_step', ''),
            item.get('latest_sample_id', ''),
            ','.join(item.get('ready_candidates', [])),
        ))
    return '\n'.join(lines)

def build_resubmit_plan(state, config=None, exclusions=None, lineage_id=None):
    if config is None:
        config = load_config()
    if exclusions is None:
        exclusions = load_exclusions()
    plan = {
        'generated_at': utc_timestamp(),
        'mode': 'dry-run',
        'lineage_filter': lineage_id,
        'resubmissions': [],
        'blocked': [],
    }
    threshold = float(config['finished_threshold_pct'])
    matched_lineages = 0
    for lineage in build_lineage_view(state, config, exclusions=exclusions):
        if lineage_id and lineage.get('lineage_id') != lineage_id:
            continue
        matched_lineages += 1
        latest_sample = lineage.get('latest_sample') or {}
        latest_sample_id = lineage.get('latest_sample_id')
        current_step = latest_sample.get('current_step')
        current_status = latest_sample.get('current_status')
        if not latest_sample_id or not current_step:
            plan['blocked'].append({
                'lineage_id': lineage['lineage_id'],
                'reason': 'missing_current_step',
            })
            continue
        if latest_sample.get('workflow_complete'):
            plan['blocked'].append({
                'lineage_id': lineage['lineage_id'],
                'latest_sample_id': latest_sample_id,
                'current_step': current_step,
                'reason': 'workflow_complete',
            })
            continue
        if latest_sample.get('ready_for_next_step'):
            plan['blocked'].append({
                'lineage_id': lineage['lineage_id'],
                'latest_sample_id': latest_sample_id,
                'current_step': current_step,
                'reason': 'already_ready_for_next_step',
            })
            continue
        step_info = latest_sample.get('steps', {}).get(current_step, {})
        crab_project_dir = step_info.get('crab_project_dir')
        if not crab_project_dir:
            plan['blocked'].append({
                'lineage_id': lineage['lineage_id'],
                'latest_sample_id': latest_sample_id,
                'current_step': current_step,
                'reason': 'missing_crab_project_dir',
            })
            continue
        finished_pct = step_info.get('finished_pct') or 0.0
        failed_pct = step_info.get('job_counts', {}).get('failed_pct') or 0.0
        publication_done_pct = step_info.get('publication_done_pct')
        publication_for_threshold = publication_done_pct or 0.0
        if publication_for_threshold + failed_pct <= threshold:
            plan['blocked'].append({
                'lineage_id': lineage['lineage_id'],
                'latest_sample_id': latest_sample_id,
                'current_step': current_step,
                'reason': 'resubmit_threshold_not_met',
                'finished_pct': finished_pct,
                'failed_pct': failed_pct,
                'publication_done_pct': publication_done_pct,
            })
            continue
        plan['resubmissions'].append({
            'lineage_id': lineage['lineage_id'],
            'latest_sample_id': latest_sample_id,
            'current_step': current_step,
            'current_status': current_status,
            'crab_project_dir': crab_project_dir,
            'finished_pct': finished_pct,
            'failed_pct': failed_pct,
            'running_pct': step_info.get('job_counts', {}).get('running_pct'),
            'publication_done_pct': publication_done_pct,
            'transferring': step_info.get('transferring'),
            'resubmit_count': step_info.get('resubmit_count', 0),
        })
    if lineage_id and matched_lineages == 0:
        plan['blocked'].append({
            'lineage_id': lineage_id,
            'reason': 'lineage_not_found_or_excluded',
        })
    return plan


def register_resubmitted_step(state, resubmission, config, resubmit_result):
    sample = state['samples'][resubmission['latest_sample_id']]
    step = resubmission['current_step']
    step_info = sample['steps'][step]
    step_info['status'] = 'RESUBMITTED'
    step_info['checked_at'] = utc_timestamp()
    step_info['last_resubmit_at'] = utc_timestamp()
    step_info['resubmit_count'] = int(step_info.get('resubmit_count', 0) or 0) + 1
    step_info['last_resubmit_stdout'] = resubmit_result.stdout.strip()
    step_info['last_resubmit_stderr'] = resubmit_result.stderr.strip()
    sample['current_status'] = 'RESUBMITTED'
    sample['ready_for_next_step'] = False
    sample['workflow_complete'] = False
    sample.pop('last_error', None)
    return sample


def execute_resubmit_plan(state, config=None, execute=False, lineage_id=None):
    if config is None:
        config = load_config()
    exclusions = load_exclusions()
    plan = build_resubmit_plan(state, config=config, exclusions=exclusions, lineage_id=lineage_id)
    plan['mode'] = 'execute' if execute else 'dry-run'
    plan['results'] = []
    if not execute:
        return state, plan
    for resubmission in plan.get('resubmissions', []):
        result = run_in_cmssw_env(
            resubmission['current_step'],
            'crab resubmit -d {0}'.format(shlex.quote(resubmission['crab_project_dir'])),
            config,
            require_proxy=True,
        )
        if result.returncode != 0:
            plan['results'].append({
                'lineage_id': resubmission['lineage_id'],
                'latest_sample_id': resubmission['latest_sample_id'],
                'current_step': resubmission['current_step'],
                'status': 'resubmit_failed',
                'return_code': result.returncode,
                'stdout': result.stdout.strip(),
                'stderr': result.stderr.strip(),
            })
            continue
        register_resubmitted_step(state, resubmission, config, result)
        plan['results'].append({
            'lineage_id': resubmission['lineage_id'],
            'latest_sample_id': resubmission['latest_sample_id'],
            'current_step': resubmission['current_step'],
            'status': 'resubmitted',
            'stdout': result.stdout.strip(),
            'stderr': result.stderr.strip(),
        })
    save_pipeline_state(state)
    return state, plan


def render_resubmit_plan(plan):
    lines = []
    lines.append('resubmit mode: {0}'.format(plan.get('mode', 'dry-run')))
    if plan.get('lineage_filter'):
        lines.append('lineage filter: {0}'.format(plan['lineage_filter']))
    lines.append('planned resubmissions: {0}'.format(len(plan.get('resubmissions', []))))
    for item in plan.get('resubmissions', []):
        lines.append('  - {0}: step={1}, sample={2}'.format(item['lineage_id'], item['current_step'], item['latest_sample_id']))
        lines.append('    finished={0}, failed={1}, publication_done={2}, running={3}, transferring={4}, resubmit_count={5}'.format(
            format_scalar(item.get('finished_pct')),
            format_scalar(item.get('failed_pct')),
            format_scalar(item.get('publication_done_pct')),
            format_scalar(item.get('running_pct')),
            format_scalar(item.get('transferring')),
            format_scalar(item.get('resubmit_count')),
        ))
        lines.append('    crab_project_dir={0}'.format(item['crab_project_dir']))
    if plan.get('results'):
        lines.append('execution results: {0}'.format(len(plan['results'])))
        for item in plan['results']:
            lines.append('  - {0}: step={1}, status={2}'.format(
                item.get('lineage_id'),
                item.get('current_step', ''),
                item.get('status'),
            ))
            if item.get('stderr'):
                lines.append('    stderr={0}'.format(item['stderr']))
    lines.append('blocked lineages: {0}'.format(len(plan.get('blocked', []))))
    for item in plan.get('blocked', []):
        lines.append('  - {0}: reason={1}, current_step={2}, latest_sample_id={3}, finished={4}, failed={5}, publication_done={6}'.format(
            item.get('lineage_id'),
            item.get('reason'),
            item.get('current_step', ''),
            item.get('latest_sample_id', ''),
            format_scalar(item.get('finished_pct')),
            format_scalar(item.get('failed_pct')),
            format_scalar(item.get('publication_done_pct')),
        ))
    return '\n'.join(lines)


def write_table(state, config=None):

    ensure_runtime_dirs()
    if config is None:
        config = load_config()
    columns = ['lineage_id'] + config['steps'] + ['current_step', 'current_status', 'ready_for_next_step', 'workflow_complete', 'latest_sample_id']
    lines = []
    lines.append('| ' + ' | '.join(columns) + ' |')
    lines.append('| ' + ' | '.join(['---'] * len(columns)) + ' |')
    lineages = build_lineage_view(state, config, exclusions=load_exclusions())
    for lineage in lineages:
        status = format_lineage_status(lineage)
        row = [lineage['lineage_id']]
        for step in config['steps']:
            row.append(lineage.get('completed_steps', {}).get(step, {}).get('output_dataset', ''))
        row.extend([
            status['current_step'],
            status['current_status'],
            status['ready_for_next_step'],
            status['workflow_complete'],
            status['latest_sample_id'],
        ])
        lines.append('| ' + ' | '.join(row) + ' |')
    with open(TABLE_FILE, 'w') as handle:
        handle.write('\n'.join(lines) + '\n')
    append_event('table', {'table_file': TABLE_FILE, 'lineages': len(lineages)})
    return TABLE_FILE
def render_active_summary(checked, skipped_ready, skipped_complete, skipped_excluded=None):
    lines = []
    lines.append('checked samples: {0}'.format(len(checked)))
    lines.append('ready-to-submit samples skipped: {0}'.format(len(skipped_ready)))
    lines.append('workflow-complete samples skipped: {0}'.format(len(skipped_complete)))
    if skipped_excluded is None:
        skipped_excluded = []
    lines.append('manually excluded lineages skipped: {0}'.format(len(skipped_excluded)))
    for item in checked:
        lines.append(
            '  - {0} ({1}): {2}, status={3}, finished={4}, {5}, running={6}, {7}, {8}, transferring={9}, complete={10}'.format(
                colorize(item.get('lineage_id', item['sample_id']), BLUE),
                colorize(item['sample_id'], BLUE),
                colorize('step={0}'.format(item['step']), BROWN),
                build_check_status_label(item),
                format_field_with_color(item.get('finished_pct')),
                colorize('publication_done={0}'.format(format_scalar(item.get('publication_done_pct'))), GREEN),
                format_field_with_color(item.get('running_pct')),
                colorize('idle={0}'.format(format_scalar(item.get('idle_pct'))), GRAY),
                colorize('failed={0}'.format(format_scalar(item.get('failed_pct'))), RED),
                format_field_with_color(item.get('transferring')),
                format_field_with_color(item.get('workflow_complete')),
            )
        )
    return '\n'.join(lines)
def render_step_report(step_payload):
    lines = []
    summary = step_payload['summary']
    lines.append('{0}: {1} projects, {2} with output dataset'.format(
        step_payload['step'],
        summary['projects_total'],
        summary['projects_with_output_dataset'],
    ))
    lines.append(
        '  jobs: running={0} retry={1} failed={2} idle={3} unsubmitted={4} transferring={5} finished={6}'.format(
            summary['jobs_running'],
            summary['jobs_retry'],
            summary['jobs_failed'],
            summary['jobs_idle'],
            summary['jobs_unsubmitted'],
            summary['jobs_transferring'],
            summary['jobs_finished'],
        )
    )
    if summary['flags']:
        lines.append('  flags: ' + ', '.join(summary['flags']))
    for record in step_payload['records']:
        lines.append(
            '  - {0}: status={1}, retry={2}, running={3}, idle={4}, transferring={5}'.format(
                record['sample_id'],
                record['status'],
                record['job_counts'].get('toRetry', 0),
                record['job_counts'].get('running', 0),
                record['job_counts'].get('idle', 0),
                record['job_counts'].get('transferring', 0),
            )
        )
        if record.get('flags'):
            lines.append('    flags=' + ','.join(record['flags']))
        if record.get('output_dataset'):
            lines.append('    output={0}'.format(record['output_dataset']))
    return '\n'.join(lines)
