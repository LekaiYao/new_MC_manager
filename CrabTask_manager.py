import os

from pipeline_core import ALLOWED_STEPS, collect_step_status

CYAN = "\033[96m"
ORANGE = "\033[38;5;214m"
RED = "\033[91m"
GREEN = "\033[92m"
GREY = "\033[90m"
RESET = "\033[0m"


def manage_crab_tasks(keyword=None):
    payload = collect_step_status(keyword, write_legacy_logs=True)
    records = payload['records']
    summary = payload['summary']
    log_filename = os.path.join('.', 'log', 'CrabTask_manager_jobStatus_{0}.log'.format(keyword))
    log_output_filename = os.path.join('.', 'txt', 'CrabTask_manager_OUTPUT_DIRs_{0}.txt'.format(keyword))

    if not records:
        print("[!] No CRAB jobs found for step '{0}'.".format(keyword))
        return payload

    for record in records:
        print("\n'crab status -d {0}'".format(record['crab_project_dir']))
        if record['status'] == 'command_failed':
            print("{0}[X] Error retrieving status.{1}".format(RED, RESET))
            continue

        if record['job_counts'].get('finished', 0):
            print("{0} {1} jobs FINISHED.{2}".format(CYAN, record['job_counts']['finished'], RESET))
        if record['job_counts'].get('running', 0):
            print("{0} {1} jobs RUNNING.    {2}".format(GREEN, record['job_counts']['running'], RESET))
        if record['job_counts'].get('transferring', 0):
            print("{0} {1} jobs TRANSFERRING.{2}".format(GREEN, record['job_counts']['transferring'], RESET))
        if record['job_counts'].get('idle', 0):
            print("{0} {1} jobs IDLE.          {2}".format(GREY, record['job_counts']['idle'], RESET))
        if record['job_counts'].get('unsubmitted', 0):
            print("{0} {1} jobs UNSUBMITTED. {2}".format(GREY, record['job_counts']['unsubmitted'], RESET))
        if record['job_counts'].get('failed', 0):
            print("{0} {1} jobs FAILED.{2}".format(RED, record['job_counts']['failed'], RESET))
            print("{0}[->] Automatic REsubmission is FALSE.{1}".format(ORANGE, RESET))
        if record['job_counts'].get('toRetry', 0):
            print("{0} {1} jobs to RETRY.{2}".format(ORANGE, record['job_counts']['toRetry'], RESET))

    print('\nAll tasks checked. See the log file for details:', log_filename)

    if summary['projects_with_output_dataset'] > 0:
        print('Output directories saved to: {0}'.format(log_output_filename))
    else:
        print('Output directories not available.')

    print()
    processed_pct = (summary['projects_with_output_dataset'] / float(summary['projects_total']) * 100.0) if summary['projects_total'] else 0.0
    print("Total CRAB TASKS {0}processed: {1:.2f}%{2}".format(CYAN, processed_pct, RESET))
    print("Total CRAB JOBS {0}  running: {1}{2}".format(GREEN, summary['jobs_running'], RESET))
    print("Total CRAB JOBS  finished: {0}{1}".format(summary['jobs_finished'], RESET))
    print("Total CRAB JOBS {0}   failed: {1}{2}".format(RED, summary['jobs_failed'], RESET))
    return payload


if __name__ == '__main__':
    import sys

    if len(sys.argv) < 2:
        print('Usage: python CrabTask_manager.py <STEP>')
        print('Allowed STEP values: GEN, SIM, DIGI, HLT, RECO, MINIAOD, NTUPLE')
        sys.exit(1)

    keyword = sys.argv[1].upper()

    if keyword not in ALLOWED_STEPS:
        print('[X] Invalid STEP: {0}'.format(keyword))
        print('Allowed STEP values: GEN, SIM, DIGI, HLT, RECO, MINIAOD, NTUPLE')
        sys.exit(1)

    manage_crab_tasks(keyword)
