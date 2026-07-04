import argparse
import sys

from pipeline_core import (
    ALLOWED_STEPS,
    STATE_FILE,
    append_event,
    collect_step_status,
    load_pipeline_state,
    render_step_report,
    save_pipeline_state,
)


def cmd_check(args):
    payload = collect_step_status(args.step, write_legacy_logs=True)
    save_pipeline_state(payload)
    append_event(
        'check',
        {
            'step': args.step,
            'projects_total': payload['summary']['projects_total'],
            'flags': payload['summary']['flags'],
        },
    )
    print(render_step_report(payload))
    print('\nstate saved to: {0}'.format(STATE_FILE))
    return 0


def cmd_report(args):
    state = load_pipeline_state()
    if not state.get('steps'):
        print('no pipeline state found at {0}'.format(STATE_FILE))
        return 1

    selected_steps = [args.step] if args.step else sorted(state['steps'].keys())
    missing_steps = [step for step in selected_steps if step not in state['steps']]
    if missing_steps:
        print('missing state for steps: ' + ', '.join(missing_steps))
        return 1

    for index, step in enumerate(selected_steps):
        if index:
            print()
        print(render_step_report(state['steps'][step]))

    append_event('report', {'steps': selected_steps})
    return 0


def build_parser():
    parser = argparse.ArgumentParser(description='new_MC_manager pipeline driver')
    subparsers = parser.add_subparsers(dest='command')
    subparsers.required = True

    check_parser = subparsers.add_parser(
        'check',
        help='run crab status for one step, update pipeline_state.json, and keep legacy logs',
    )
    check_parser.add_argument('step', choices=sorted(ALLOWED_STEPS))
    check_parser.set_defaults(func=cmd_check)

    report_parser = subparsers.add_parser(
        'report',
        help='print a summary from pipeline_state.json',
    )
    report_parser.add_argument('--step', choices=sorted(ALLOWED_STEPS))
    report_parser.set_defaults(func=cmd_report)

    return parser


def main(argv=None):
    parser = build_parser()
    args = parser.parse_args(argv)
    return args.func(args)


if __name__ == '__main__':
    sys.exit(main())
