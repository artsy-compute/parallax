from typing import Any


def build_node_lifecycle(
    *,
    management_mode: str,
    process_status: dict[str, Any] | None,
    scheduler_joined: bool,
    scheduler_membership: str | None = None,
    scheduler_node_id: str | None = None,
    runtime_status: str | None = None,
    serving_start_layer: int | None = None,
    serving_end_layer: int | None = None,
    serving_total_layers: int | None = None,
) -> dict[str, Any]:
    process = dict(process_status or {})
    runtime_status = str(runtime_status or '')
    process_running = bool(process.get('running'))
    process_confirmed_running = bool(process.get('confirmed_running', process_running))
    process_source = str(process.get('source') or '')
    process_message = str(process.get('message') or '')
    checked_at = float(process.get('checked_at') or 0.0)

    action_state = process_source if process_source in {'action', 'action_pending'} else 'none'
    action_message = process_message if action_state != 'none' else ''

    if process_running:
        process_state = 'running' if process_confirmed_running else 'starting'
    else:
        process_state = 'stopped'
    if process_source == 'probe_error':
        process_state = 'unknown'

    membership = str(scheduler_membership or ('joined' if scheduler_joined else 'not_joined'))
    if action_state == 'action' and not process_running:
        membership = 'leaving'
    elif action_state == 'action_pending' and process_running and not scheduler_joined:
        membership = 'joining'
    elif runtime_status and membership in {'joined', 'not_joined'} and runtime_status not in {'available', 'waiting'}:
        membership = runtime_status

    if scheduler_joined and isinstance(serving_start_layer, int) and isinstance(serving_end_layer, int):
        serving_state = 'active' if runtime_status == 'available' else 'assigned'
    elif scheduler_joined:
        serving_state = 'joined'
    else:
        serving_state = 'unassigned'

    summary = 'Stopped'
    if membership == 'joining' or process_state == 'starting':
        summary = 'Joining scheduler'
    elif membership == 'leaving':
        summary = 'Stopped, waiting for scheduler timeout'
    elif membership == 'joined' and serving_state == 'active':
        summary = 'Serving'
    elif membership == 'joined':
        summary = 'Joined'
    elif process_state == 'running':
        summary = 'Process running'

    return {
        'summary': summary,
        'management': {
            'mode': management_mode,
            'last_action_state': action_state,
            'last_action_message': action_message,
            'checked_at': checked_at,
        },
        'process': {
            'state': process_state,
            'pid': str(process.get('pid') or ''),
            'source': process_source,
            'message': process_message,
            'checked_at': checked_at,
        },
        'scheduler': {
            'membership': membership,
            'node_id': scheduler_node_id,
            'status': runtime_status or None,
            'joined': scheduler_joined,
        },
        'serving': {
            'state': serving_state,
            'start_layer': serving_start_layer,
            'end_layer': serving_end_layer,
            'total_layers': serving_total_layers,
        },
    }
