import datetime
import json
import os
import re
import subprocess


ALLOWED_STEPS = {"GEN", "SIM", "DIGI", "HLT", "RECO", "MINIAOD", "NTUPLE"}

STEP_DIR_MAP = {
    "GEN": "../CMSSW_10_6_20_patch1/src/crab_projects",
    "SIM": "../CMSSW_10_6_17_patch1/src/crab_projects",
    "DIGI": "../CMSSW_10_6_17_patch1/src/crab_projects",
    "HLT": "../CMSSW_10_2_16_UL/src/crab_projects",
    "RECO": "../CMSSW_10_6_17_patch1/src/crab_projects",
    "MINIAOD": "../CMSSW_10_6_17_patch1/src/crab_projects",
    "NTUPLE": "../CMSSW_10_6_20/src/NtupleMaker/NtupleMaker/test/crab_projects",
}

MANAGER_DIR = os.path.dirname(os.path.abspath(__file__))
STATE_DIR = os.path.join(MANAGER_DIR, 'state')
LOG_DIR = os.path.join(MANAGER_DIR, 'log')
TXT_DIR = os.path.join(MANAGER_DIR, 'txt')
STATE_FILE = os.path.join(STATE_DIR, 'pipeline_state.json')
EVENTS_FILE = os.path.join(LOG_DIR, 'pipeline_events.jsonl')


def ensure_runtime_dirs():
    for path in (LOG_DIR, TXT_DIR, STATE_DIR):
        if not os.path.isdir(path):
            os.makedirs(path)


def utc_timestamp():
    return datetime.datetime.utcnow().isoformat() + 'Z'


def parse_jobs_count(raw_value):
    if raw_value is None:
        return 0
    value = raw_value.strip()
    if '/' in value:
        value = value.split('/')[0]
    match = re.search(r'(\d+)', value)
    return int(match.group(1)) if match else 0


def strip_ansi(text):
    return re.sub(r'\x1b\[[0-9;]*m', '', text)


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


def get_crab_job_dirs(step):
    crab_jobs_dir = os.path.abspath(os.path.join(MANAGER_DIR, STEP_DIR_MAP[step]))
    if not os.path.isdir(crab_jobs_dir):
        return []
    entries = []
    for entry in os.listdir(crab_jobs_dir):
        full_path = os.path.join(crab_jobs_dir, entry)
        if os.path.isdir(full_path) and step in entry:
            entries.append(full_path)
    return sorted(entries)


def run_crab_status(job_dir, long_format=False):
    cmd = ['crab', 'status']
    if long_format:
        cmd.append('--long')
    cmd.extend(['-d', job_dir])
    return subprocess.run(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        universal_newlines=True,
    )


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


def collect_step_status(step, write_legacy_logs=True):
    ensure_runtime_dirs()

    if step not in ALLOWED_STEPS:
        raise ValueError('Invalid step: {0}'.format(step))

    job_dirs = get_crab_job_dirs(step)
    step_payload = {
        'step': step,
        'generated_at': utc_timestamp(),
        'crab_projects_dir': os.path.abspath(os.path.join(MANAGER_DIR, STEP_DIR_MAP[step])),
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
        result = run_crab_status(job_dir)
        legacy_log_lines.append('')
        legacy_log_lines.append('Checking status of {0}...'.format(job_dir))

        if result.returncode != 0:
            step_payload['records'].append({
                'step': step,
                'crab_project_dir': job_dir,
                'crab_project_name': os.path.basename(job_dir),
                'sample_id': os.path.basename(job_dir).replace('crab_', '', 1),
                'status': 'command_failed',
                'command': 'crab status',
                'return_code': result.returncode,
                'stderr': result.stderr.strip(),
                'generated_at': step_payload['generated_at'],
                'flags': ['status_command_failed'],
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
            'sample_id': infer_sample_id(step, os.path.basename(job_dir), output_dataset or ''),
            'status': parsed['scheduler_status'] or parsed['crab_server_status'] or 'UNKNOWN',
            'task_name': parsed['task_name'],
            'dashboard_url': parsed['dashboard_url'],
            'output_dataset': output_dataset,
            'warnings': parsed['warnings'],
            'job_counts': parsed['job_counts'],
            'resource_summary': parsed['resource_summary'],
            'flags': [],
        }
        record['flags'] = classify_flags(record)

        step_payload['records'].append(record)
        step_payload['summary']['jobs_finished'] += record['job_counts']['finished']
        step_payload['summary']['jobs_running'] += record['job_counts']['running']
        step_payload['summary']['jobs_failed'] += record['job_counts']['failed']
        step_payload['summary']['jobs_retry'] += record['job_counts']['toRetry']
        step_payload['summary']['jobs_idle'] += record['job_counts']['idle']
        step_payload['summary']['jobs_unsubmitted'] += record['job_counts']['unsubmitted']
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


def load_pipeline_state():
    if not os.path.exists(STATE_FILE):
        return {'generated_at': None, 'steps': {}}
    with open(STATE_FILE, 'r') as handle:
        return json.load(handle)


def save_pipeline_state(step_payload):
    ensure_runtime_dirs()
    state = load_pipeline_state()
    state['generated_at'] = utc_timestamp()
    state.setdefault('steps', {})
    state['steps'][step_payload['step']] = step_payload
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


def render_step_report(step_payload):
    lines = []
    summary = step_payload['summary']
    lines.append('{0}: {1} projects, {2} with output dataset'.format(
        step_payload['step'],
        summary['projects_total'],
        summary['projects_with_output_dataset'],
    ))
    lines.append(
        '  jobs: running={0} retry={1} failed={2} idle={3} unsubmitted={4} finished={5}'.format(
            summary['jobs_running'],
            summary['jobs_retry'],
            summary['jobs_failed'],
            summary['jobs_idle'],
            summary['jobs_unsubmitted'],
            summary['jobs_finished'],
        )
    )
    if summary['flags']:
        lines.append('  flags: ' + ', '.join(summary['flags']))

    for record in step_payload['records']:
        lines.append(
            '  - {0}: status={1}, retry={2}, running={3}, idle={4}'.format(
                record['sample_id'],
                record['status'],
                record['job_counts'].get('toRetry', 0),
                record['job_counts'].get('running', 0),
                record['job_counts'].get('idle', 0),
            )
        )
        if record.get('flags'):
            lines.append('    flags=' + ','.join(record['flags']))
        if record.get('output_dataset'):
            lines.append('    output={0}'.format(record['output_dataset']))

    return '\n'.join(lines)
