import datetime
import json
import os
import re
import shlex
import subprocess
import tempfile
MANAGER_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_FILE = os.path.join(MANAGER_DIR, 'config.json')
LOCAL_VOMS_FILE = os.path.join(MANAGER_DIR, '.local', 'v.json')
STATE_DIR = os.path.join(MANAGER_DIR, 'state')
LOG_DIR = os.path.join(MANAGER_DIR, 'log')
TXT_DIR = os.path.join(MANAGER_DIR, 'txt')
STATE_FILE = os.path.join(STATE_DIR, 'pipeline_state.json')
EVENTS_FILE = os.path.join(LOG_DIR, 'pipeline_events.jsonl')
TABLE_FILE = os.path.join(STATE_DIR, 'pipeline_table.md')
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
def load_voms_password():
    if not os.path.exists(LOCAL_VOMS_FILE):
        raise RuntimeError('Missing local VOMS password file: {0}'.format(LOCAL_VOMS_FILE))
    with open(LOCAL_VOMS_FILE, 'r') as handle:
        payload = json.load(handle)
    password = payload.get('voms_password')
    if not password:
        raise RuntimeError('Missing voms_password in {0}'.format(LOCAL_VOMS_FILE))
    return password
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
        'sample_id': None,
    }
    if not os.path.exists(crab_log):
        return metadata
    with open(crab_log, 'r') as handle:
        content = handle.read()
    request_match = re.search(r"config\.General\.requestName = '([^']+)'", content)
    tag_match = re.search(r"config\.Data\.outputDatasetTag = '([^']+)'", content)
    if request_match:
        metadata['request_name'] = request_match.group(1)
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
    password = load_voms_password()
    env = os.environ.copy()
    env['NEW_MC_VOMS_PASSWORD'] = password
    script_body = (
        'if ! voms-proxy-info -exists -valid 12:00 >/dev/null 2>&1; then\n'
        '  printf %s\\n "$NEW_MC_VOMS_PASSWORD" | voms-proxy-init --rfc --voms cms --valid 192:00 >/dev/null\n'
        'fi\n'
        'voms-proxy-info -timeleft\n'
    )
    result = run_cmssw_script(step, script_body, config, env=env)
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
    }
    counts_pattern = re.compile(
        r'(idle|running|toRetry|unsubmitted|finished|failed|transferring)\s+([0-9.]+)%\s+\(([^)]+)\)'
    )
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
def merge_discovered_samples(state, config):
    active_steps = get_active_steps(config)
    for step in active_steps:
        for job_dir in get_crab_job_dirs(step, config):
            metadata = extract_metadata_from_crab_log(job_dir, step)
            sample_id = metadata['sample_id']
            if not sample_id:
                sample_id = infer_sample_id(step, os.path.basename(job_dir), '')
            sample = state['samples'].setdefault(sample_id, ensure_sample_state(sample_id, config))
            step_info = sample['steps'].setdefault(step, {})
            step_info.setdefault('step', step)
            step_info['crab_project_dir'] = job_dir
            step_info['crab_project_name'] = os.path.basename(job_dir)
            if metadata['request_name']:
                step_info['request_name'] = metadata['request_name']
            if metadata['output_dataset_tag']:
                step_info['output_dataset_tag'] = metadata['output_dataset_tag']
            if sample['current_step'] is None or step_index(step, config) > step_index(sample['current_step'], config):
                sample['current_step'] = step
                sample['next_step'] = get_next_step(step, config)
                if not sample.get('workflow_complete'):
                    sample['current_status'] = sample.get('current_status') or 'DISCOVERED'
    return state
def step_completed(record, config):
    finished_pct = record['job_counts'].get('finished_pct') or 0.0
    transferring = record['job_counts'].get('transferring') or 0
    return finished_pct >= float(config['finished_threshold_pct']) and transferring == 0
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
        'output_dataset_tag': record.get('output_dataset_tag'),
        'finished_pct': record.get('job_counts', {}).get('finished_pct'),
        'transferring': record.get('job_counts', {}).get('transferring'),
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
    state = load_pipeline_state(config)
    state = merge_discovered_samples(state, config)
    checked = []
    skipped_ready = []
    skipped_complete = []
    for lineage in build_lineage_view(state, config):
        lineage_id = lineage['lineage_id']
        sample_id = lineage.get('latest_sample_id')
        sample = lineage.get('latest_sample')
        if not sample_id or sample is None:
            continue
        if sample.get('workflow_complete'):
            skipped_complete.append(lineage_id)
            continue
        if sample.get('ready_for_next_step'):
            skipped_ready.append(lineage_id)
            continue
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
        checked.append({
            'lineage_id': lineage_id,
            'sample_id': sample_id,
            'step': current_step,
            'status': sample['current_status'],
            'finished_pct': record['job_counts'].get('finished_pct'),
            'transferring': record['job_counts'].get('transferring'),
            'ready_for_next_step': sample['ready_for_next_step'],
            'workflow_complete': sample['workflow_complete'],
        })
    save_pipeline_state(state)
    append_event('check-active', {
        'checked_samples': checked,
        'skipped_ready': skipped_ready,
        'skipped_complete': skipped_complete,
        'terminal_step': config['terminal_step'],
        'mode': 'lineage-latest',
    })
    return state, checked, skipped_ready, skipped_complete
def infer_lineage_id(sample_id):
    return re.sub(r'_v\d+_', '_', sample_id, count=1)

def sample_version(sample_id):
    match = re.search(r'_v(\d+)_', sample_id)
    return int(match.group(1)) if match else 0

def build_lineage_view(state, config=None):
    if config is None:
        config = load_config()
    lineages = {}
    for sample_id, sample in state.get('samples', {}).items():
        lineage_id = infer_lineage_id(sample_id)
        entry = lineages.setdefault(lineage_id, {
            'lineage_id': lineage_id,
            'sample_ids': [],
            'latest_sample_id': None,
            'latest_sample': None,
            'steps': {},
            'completed_steps': {},
            'ready_candidates': [],
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
        if sample.get('ready_for_next_step'):
            entry['ready_candidates'].append(sample_id)
        for step, step_info in sample.get('steps', {}).items():
            current = entry['steps'].get(step)
            candidate_key = (sample_version(sample_id), sample_id)
            current_key = (-1, '') if current is None else (sample_version(current['sample_id']), current['sample_id'])
            merged = dict(step_info)
            merged['sample_id'] = sample_id
            if current is None or candidate_key >= current_key:
                entry['steps'][step] = merged
            if step_info.get('completed') and step_info.get('output_dataset'):
                completed_current = entry['completed_steps'].get(step)
                completed_key = (-1, '') if completed_current is None else (sample_version(completed_current['sample_id']), completed_current['sample_id'])
                if completed_current is None or candidate_key >= completed_key:
                    entry['completed_steps'][step] = merged
    return [lineages[key] for key in sorted(lineages.keys())]

def format_bool(value):
    return 'yes' if value else 'no'

def format_lineage_status(lineage):
    latest_sample = lineage.get('latest_sample') or {}
    return {
        'current_step': latest_sample.get('current_step') or '',
        'current_status': latest_sample.get('current_status') or '',
        'ready_for_next_step': format_bool(latest_sample.get('ready_for_next_step')),
        'workflow_complete': format_bool(latest_sample.get('workflow_complete')),
        'latest_sample_id': lineage.get('latest_sample_id') or '',
    }

def build_submit_plan(state, config=None):
    if config is None:
        config = load_config()
    plan = {
        'generated_at': utc_timestamp(),
        'mode': 'dry-run',
        'submissions': [],
        'blocked': [],
    }
    for lineage in build_lineage_view(state, config):
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
        processed_dataset = _extract_processed_dataset(input_dataset)
        next_dataset_name = processed_dataset.replace(current_step, next_step, 1) if processed_dataset else None
        plan['submissions'].append({
            'lineage_id': lineage['lineage_id'],
            'latest_sample_id': latest_sample_id,
            'current_step': current_step,
            'next_step': next_step,
            'input_dataset': input_dataset,
            'request_name': next_dataset_name,
            'output_dataset_tag': next_dataset_name,
            'work_dir': get_step_dir(next_step, config),
            'crab_config_file': os.path.join(get_step_dir(next_step, config), 'crab3_Config.py'),
            'pset_name': 'BPH_{0}_13TeV_cfg.py'.format(next_step) if next_step != 'NTUPLE' else None,
        })
    return plan

def render_submit_plan(plan):
    lines = []
    lines.append('submit-next mode: {0}'.format(plan.get('mode', 'dry-run')))
    lines.append('planned submissions: {0}'.format(len(plan.get('submissions', []))))
    for item in plan.get('submissions', []):
        lines.append('  - {0}: {1} -> {2}'.format(item['lineage_id'], item['current_step'], item['next_step']))
        lines.append('    latest_sample_id={0}'.format(item['latest_sample_id']))
        lines.append('    input_dataset={0}'.format(item['input_dataset']))
        lines.append('    requestName={0}'.format(item['request_name']))
        lines.append('    crab_config={0}'.format(item['crab_config_file']))
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

def write_table(state, config=None):
    ensure_runtime_dirs()
    if config is None:
        config = load_config()
    columns = ['lineage_id'] + config['steps'] + ['current_step', 'current_status', 'ready_for_next_step', 'workflow_complete', 'latest_sample_id']
    lines = []
    lines.append('| ' + ' | '.join(columns) + ' |')
    lines.append('| ' + ' | '.join(['---'] * len(columns)) + ' |')
    lineages = build_lineage_view(state, config)
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
def render_active_summary(checked, skipped_ready, skipped_complete):
    lines = []
    lines.append('checked samples: {0}'.format(len(checked)))
    lines.append('ready-to-submit samples skipped: {0}'.format(len(skipped_ready)))
    lines.append('workflow-complete samples skipped: {0}'.format(len(skipped_complete)))
    for item in checked:
        lines.append(
            '  - {0} ({1}): step={2}, status={3}, finished={4}, transferring={5}, ready={6}, complete={7}'.format(
                item.get('lineage_id', item['sample_id']),
                item['sample_id'],
                item['step'],
                item['status'],
                item.get('finished_pct'),
                item.get('transferring'),
                item.get('ready_for_next_step'),
                item.get('workflow_complete'),
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
