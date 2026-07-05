import argparse
import sys

from pipeline_core import (
    STATE_FILE,
    TABLE_FILE,
    append_event,
    execute_submit_plan,
    check_active_samples,
    collect_step_status,
    load_config,
    load_pipeline_state,
    render_active_summary,
    render_step_report,
    render_submit_plan,
    write_table,
)


def cmd_check(args):
    config = load_config()
    payload = collect_step_status(args.step, write_legacy_logs=True, config=config)
    append_event(
        'check',
        {
            'step': args.step,
            'projects_total': payload['summary']['projects_total'],
            'flags': payload['summary']['flags'],
        },
    )
    print(render_step_report(payload))
    return 0


def cmd_check_active(args):
    config = load_config()
    state, checked, skipped_ready, skipped_complete, skipped_excluded = check_active_samples(config=config)
    print(render_active_summary(checked, skipped_ready, skipped_complete, skipped_excluded))
    print('\nstate saved to: {0}'.format(STATE_FILE))
    return 0


def cmd_report(args):
    state = load_pipeline_state(load_config())
    print('samples in state: {0}'.format(len(state.get('samples', {}))))
    for sample_id in sorted(state.get('samples', {}).keys()):
        sample = state['samples'][sample_id]
        print('{0}: current_step={1}, current_status={2}, ready={3}, complete={4}'.format(
            sample_id,
            sample.get('current_step'),
            sample.get('current_status'),
            sample.get('ready_for_next_step'),
            sample.get('workflow_complete'),
        ))
    return 0


def cmd_table(args):
    config = load_config()
    state = load_pipeline_state(config)
    table_file = write_table(state, config=config)
    print('table written to: {0}'.format(table_file))
    return 0


def cmd_submit_next(args):
    config = load_config()
    state = load_pipeline_state(config)
    state, plan = execute_submit_plan(state, config=config, execute=args.execute)
    append_event(
        'submit-next',
        {
            'mode': plan.get('mode'),
            'planned_submissions': len(plan.get('submissions', [])),
            'blocked_lineages': len(plan.get('blocked', [])),
            'results': len(plan.get('results', [])),
        },
    )
    print(render_submit_plan(plan))
    if args.execute:
        print('\nstate saved to: {0}'.format(STATE_FILE))
    return 0


def build_parser():
    parser = argparse.ArgumentParser(description='new_MC_manager pipeline driver')
    subparsers = parser.add_subparsers(dest='command')
    subparsers.required = True

    check_parser = subparsers.add_parser(
        'check',
        help='run crab status for one step and keep legacy logs',
    )
    check_parser.add_argument('step', choices=sorted(load_config()['allowed_steps']))
    check_parser.set_defaults(func=cmd_check)

    active_parser = subparsers.add_parser(
        'check-active',
        help='check only the latest unfinished step for each sample',
    )
    active_parser.set_defaults(func=cmd_check_active)

    report_parser = subparsers.add_parser(
        'report',
        help='print a compact per-sample state summary',
    )
    report_parser.set_defaults(func=cmd_report)

    table_parser = subparsers.add_parser(
        'table',
        help='write a Markdown table of completed-step output datasets',
    )
    table_parser.set_defaults(func=cmd_table)

    submit_parser = subparsers.add_parser(
        'submit-next',
        help='plan or execute the next-step submissions of ready lineages',
    )
    submit_parser.add_argument('--execute', action='store_true', help='perform real crab submit calls and update state on success')
    submit_parser.set_defaults(func=cmd_submit_next)

    return parser


def main(argv=None):
    parser = build_parser()
    args = parser.parse_args(argv)
    return args.func(args)


if __name__ == '__main__':
    sys.exit(main())
