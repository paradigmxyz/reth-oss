"""HTTP contract regressions for finalized devnet verification."""

from contextlib import contextmanager, redirect_stdout, ExitStack
from copy import deepcopy
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import io
import json
from pathlib import Path
import tempfile
import threading
import time
import unittest
from unittest.mock import patch
from urllib.parse import urlsplit
from types import SimpleNamespace

from beacon import execution_payload, fixed_beacon
from contracts import CheckError, Endpoint, FAR_FUTURE_EPOCH, FORKS, Header, HttpTransport, NotReady, required_forks, schedule
from execution import compare_execution, finalized_el
from provenance import deployment_provenance
from verify import READINESS_FIELDS, check_planned, main, merge_contract, observe_fork, readiness_gate

CHECKS = Path(__file__).resolve().parent
ROOT = '0x' + '11' * 32
STATE = '0x' + '22' * 32
PARENT = '0x' + '33' * 32
HASH = '0x' + '44' * 32
RECEIPTS = '0x' + '55' * 32
TX = '0x' + '66' * 32
ADDRESS = '0x' + '77' * 20
SIGNATURE = '0x' + '88' * 96
BLOOM = '0x' + '00' * 256


@contextmanager
def server(route):
    calls = []

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            self.respond(None)

        def do_POST(self):
            self.respond(json.loads(self.rfile.read(int(self.headers['Content-Length']))))

        def respond(self, payload):
            calls.append((self.path, deepcopy(payload)))
            status, value = route(self.path, payload)
            raw = value if isinstance(value, bytes) else json.dumps(value).encode()
            self.send_response(status)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Content-Length', str(len(raw)))
            self.end_headers()
            self.wfile.write(raw)

        def log_message(self, *args):
            pass

    httpd = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
    thread = threading.Thread(target=lambda: httpd.serve_forever(poll_interval=0.01), daemon=True)
    thread.start()
    try:
        yield f'http://127.0.0.1:{httpd.server_port}', calls
    finally:
        httpd.shutdown()
        httpd.server_close()
        thread.join()


def endpoint(url, role='test'):
    return Endpoint(url, role, HttpTransport(time.monotonic() + 10))


def config_routes(epochs=None):
    epochs = epochs or [0, 2, 4, 6, 8, 10, 12, FAR_FUTURE_EPOCH]
    versions = ['0x' + str(index + 1) + '0000038' for index in range(8)]
    spec = {'SLOTS_PER_EPOCH': '32', 'SLOT_DURATION_MS': '3000', 'GENESIS_FORK_VERSION': versions[0]}
    forks = []
    for index, name in enumerate(FORKS):
        if index:
            spec[name.upper() + '_FORK_EPOCH'] = str(epochs[index])
            spec[name.upper() + '_FORK_VERSION'] = versions[index]
        forks.append({'previous_version': versions[max(0, index - 1)], 'current_version': versions[index], 'epoch': str(epochs[index])})
    genesis = {'genesis_time': '1', 'genesis_validators_root': ROOT, 'genesis_fork_version': versions[0]}
    return {'/eth/v1/config/spec': {'data': spec}, '/eth/v1/config/fork_schedule': {'data': forks}, '/eth/v1/beacon/genesis': {'data': genesis}}


def payload(version='deneb', height=7):
    value = {'parent_hash': PARENT, 'state_root': STATE, 'receipts_root': RECEIPTS, 'block_hash': HASH, 'prev_randao': ROOT, 'fee_recipient': ADDRESS, 'logs_bloom': BLOOM, 'extra_data': '0x', 'base_fee_per_gas': '7', 'block_number': str(height), 'gas_limit': '30000000', 'gas_used': '21000', 'timestamp': '100', 'transactions': ['0x01']}
    if version != 'bellatrix':
        value['withdrawals'] = []
    if version in ('deneb', 'electra', 'fulu', 'gloas'):
        value.update({'blob_gas_used': '0', 'excess_blob_gas': '0'})
    if version == 'gloas':
        value.update({'slot_number': '64', 'block_access_list': '0x'})
    return value


def block_response(version='deneb', slot=64):
    body = {'sync_aggregate': {}}
    if version in ('phase0', 'altair'):
        if version == 'phase0':
            body = {}
    elif version == 'gloas':
        body.update({'execution_payload_bid': {'slot': str(slot), 'parent_block_root': PARENT, 'block_hash': HASH}, 'parent_execution_requests': {}, 'payload_attestations': []})
    else:
        body['execution_payload'] = payload(version)
        if version in ('deneb', 'electra', 'fulu'):
            body['blob_kzg_commitments'] = []
        if version in ('electra', 'fulu'):
            body['execution_requests'] = {}
    return {'version': version, 'execution_optimistic': False, 'finalized': True, 'data': {'message': {'slot': str(slot), 'parent_root': PARENT, 'state_root': STATE, 'body': body}, 'signature': SIGNATURE}}


def envelope():
    return {'version': 'gloas', 'execution_optimistic': False, 'finalized': True, 'data': {'signature': SIGNATURE, 'message': {'beacon_block_root': ROOT, 'execution_requests': {}, 'payload': payload('gloas')}}}


def execution_block(height=7, version="deneb"):
    value = {'number': hex(height), 'hash': HASH, 'parentHash': PARENT, 'stateRoot': STATE, 'receiptsRoot': RECEIPTS, 'transactions': [TX], 'gasLimit': hex(30000000), 'gasUsed': hex(21000), 'timestamp': hex(100), 'baseFeePerGas': '0x7', 'blobGasUsed': '0x0', 'excessBlobGas': '0x0', 'withdrawals': []}
    if version not in ('deneb', 'electra', 'fulu', 'gloas'):
        del value['blobGasUsed'], value['excessBlobGas']
    if version == 'bellatrix':
        del value['withdrawals']
    return value


def receipt(height=7):
    return {'transactionHash': TX, 'transactionIndex': '0x0', 'blockHash': HASH, 'blockNumber': hex(height), 'from': ADDRESS, 'to': ADDRESS, 'cumulativeGasUsed': '0x5208', 'gasUsed': '0x5208', 'contractAddress': None, 'logs': [], 'logsBloom': BLOOM, 'status': '0x1', 'type': '0x0', 'effectiveGasPrice': '0x7'}


def rpc_route(path, request):
    method, params = request['method'], request['params']
    if method == 'eth_getBlockByNumber':
        result = execution_block(9 if params[0] == 'finalized' else int(params[0], 16))
    elif method == 'eth_getTransactionReceipt':
        result = receipt()
    elif method == 'eth_getRawTransactionByHash':
        result = '0x01'
    else:
        result = '0x1'
    return 200, {'jsonrpc': '2.0', 'id': request['id'], 'result': result}


def independent_fixture(urls):
    names = {'reference_execution': 'ref-el', 'reference_beacon': 'ref-beacon', 'sut': 'embedded-beacon-sut'}
    images = {role: 'sha256:' + str(index + 1) * 64 for index, role in enumerate(names)}
    expected = {'schema_version': 1, 'enclave_uuid': 'ab' * 16, 'roles': names, 'allowed_services': list(names.values()), 'images': images, 'ports': {'reference_execution': '8545/tcp', 'reference_beacon': '3500/tcp', 'sut_execution': '8545/tcp', 'sut_beacon': '5052/tcp'}}
    containers = []
    for index, (role, name) in enumerate(names.items()):
        keys = ('sut_execution', 'sut_beacon') if role == 'sut' else (role,)
        ports = {expected['ports'][key]: [{'HostIp': '127.0.0.1', 'HostPort': str(urlsplit(urls[key]).port)}] for key in keys}
        containers.append({'Id': str(index + 1) * 64, 'Image': images[role], 'State': {'Running': True}, 'Config': {'Labels': {'com.kurtosistech.enclave-id': expected['enclave_uuid'], 'com.kurtosistech.container-type': 'user-service', 'com.kurtosistech.id': name, 'kurtosis_service_name': name}, 'Entrypoint': ['/node'], 'Cmd': ['node', '--embedded-beacon'] if role == 'sut' else ['node']}, 'NetworkSettings': {'Ports': ports}})
    def inspect(arguments):
        return '\n'.join(item['Id'] for item in containers) + '\n' if arguments[0] == 'ps' else json.dumps(containers)
    return expected, containers, inspect


class HttpContracts(unittest.TestCase):
    def test_malformed_json_and_http_errors_do_not_expose_body(self):
        for status, raw, expected in [(200, b'{secret-token', 'test returned malformed JSON'), (401, b'secret-token', 'test request failed at /probe (HTTP 401)')]:
            with self.subTest(status=status), server(lambda path, req: (status, raw)) as (url, calls):
                with self.assertRaisesRegex(CheckError, '^' + expected.replace('(', r'\(').replace(')', r'\)') + '$'):
                    endpoint(url).beacon('/probe')
                self.assertEqual(len(calls), 1)

    def test_http_diagnostic_excludes_query_fragment_and_response_secrets(self):
        with server(lambda path, req: (404, b'server-secret')) as (url, _):
            with self.assertRaises(CheckError) as caught:
                endpoint(url).beacon('/eth/v1/beacon/states/1/root?token=query-secret#fragment-secret')
            self.assertEqual(str(caught.exception), 'test request failed at /eth/v1/beacon/states/1/root (HTTP 404)')
    def test_rpc_error_is_sanitized(self):
        with server(lambda path, req: (200, {'jsonrpc': '2.0', 'id': req['id'], 'error': {'code': -32000, 'message': 'private-token', 'data': {'secret': 'token'}}})) as (url, _):
            with self.assertRaises(CheckError) as caught:
                endpoint(url).rpc('eth_chainId', [])
            self.assertEqual(str(caught.exception), 'test RPC eth_chainId failed (code -32000)')

    def test_rpc_wrong_identifier_and_boolean_error_code_fail(self):
        for value in [{'id': True, 'result': '0x1'}, {'id': 1, 'error': {'code': True}}]:
            with self.subTest(value=value), server(lambda path, req: (200, {'jsonrpc': '2.0', **value})) as (url, _):
                with self.assertRaises(CheckError):
                    endpoint(url).rpc('eth_chainId', [])

    def test_retryable_transport_and_absent_block_are_distinct(self):
        with server(lambda path, req: (503 if path == '/retry' else 404, {})) as (url, _):
            node = endpoint(url)
            with self.assertRaises(NotReady):
                node.beacon('/retry')
            self.assertIsNone(node.beacon('/missing', missing_ok=True))
            with self.assertRaises(CheckError):
                node.beacon('/missing')


class ScheduleContracts(unittest.TestCase):
    def deployed(self, routes):
        with server(lambda path, req: (200, routes[path])) as (url, _):
            return schedule(endpoint(url))

    def test_deployed_profile_matches_checked_in_historical_plan(self):
        path = CHECKS.parent / 'package/schedules/historical.json'
        plan = json.loads(path.read_text())
        routes = config_routes([plan['fork_epochs'][name] for name in FORKS])
        routes['/eth/v1/config/spec']['data']['SLOT_DURATION_MS'] = str(plan['slot_duration_ms'])
        config = self.deployed(routes)
        names = list(FORKS)
        check_planned(config, path, names)
        self.assertEqual([item['name'] for item in required_forks(config, names)], names)

    def test_disabled_and_superseded_forks_cannot_claim_coverage(self):
        disabled = self.deployed(config_routes())
        with self.assertRaisesRegex(CheckError, '^required fork gloas is unsupported or unscheduled$'):
            required_forks(disabled, ['gloas'])
        genesis = self.deployed(config_routes([0] * 8))
        with self.assertRaisesRegex(CheckError, '^required fork phase0 was superseded at genesis and needs a separate historical case$'):
            required_forks(genesis, ['phase0'])
        self.assertEqual(required_forks(genesis, ['gloas'])[0]['epoch'], 0)

    def test_config_api_schedule_disagreement_fails(self):
        routes = config_routes()
        routes['/eth/v1/config/fork_schedule']['data'][4]['epoch'] = '9'
        with self.assertRaisesRegex(CheckError, '^fork schedule API disagrees with spec for deneb$'):
            self.deployed(routes)

    def test_incomplete_plan_cannot_mask_unexpected_deployed_fork(self):
        config = self.deployed(config_routes())
        with tempfile.TemporaryDirectory(dir=CHECKS) as directory:
            planned = {'genesis_fork': 'phase0', 'slots_per_epoch': 32, 'slot_duration_ms': 3000, 'expected_forks': ['phase0'], 'forks': [config['forks'][0]]}
            path = Path(directory) / 'plan.json'
            path.write_text(json.dumps(planned), encoding='utf-8')
            with self.assertRaisesRegex(CheckError, '^planned schedule must describe every deployed fork exactly once$'):
                check_planned(config, path, ['phase0'])

class BeaconContracts(unittest.TestCase):
    def test_deneb_missing_blob_fields_and_capella_missing_withdrawals_fail(self):
        for version, missing in [('deneb', 'blob_gas_used'), ('capella', 'withdrawals')]:
            response = block_response(version)
            del response['data']['message']['body']['execution_payload'][missing]
            with self.subTest(version=version), self.assertRaises(CheckError):
                execution_payload(None, response, Header(64, ROOT))

    def test_premerge_payload_cannot_count_as_historical_coverage(self):
        response = block_response('altair')
        response['data']['message']['body']['execution_payload'] = payload()
        with self.assertRaisesRegex(CheckError, '^pre-merge block unexpectedly contains an execution payload$'):
            execution_payload(None, response, Header(64, ROOT))

    def test_gloas_requires_linked_full_envelope_not_old_inline_payload(self):
        for root in [ROOT, STATE]:
            value = envelope()
            value['data']['message']['beacon_block_root'] = root
            with self.subTest(root=root), server(lambda path, req: (200, value)) as (url, calls):
                if root == ROOT:
                    result = execution_payload(endpoint(url), block_response('gloas'), Header(64, ROOT))
                    self.assertEqual(result['block_hash'], HASH)
                    self.assertEqual(calls[0][0], '/eth/v1/beacon/execution_payload_envelopes/' + ROOT)
                else:
                    with self.assertRaisesRegex(CheckError, '^Gloas envelope links to a different beacon block$'):
                        execution_payload(endpoint(url), block_response('gloas'), Header(64, ROOT))
        inline = block_response('gloas')
        inline['data']['message']['body']['execution_payload'] = payload('gloas')
        with self.assertRaisesRegex(CheckError, '^Gloas block must carry a bid instead of an inline payload$'):
            execution_payload(None, inline, Header(64, ROOT))

    def test_absent_gloas_envelope_is_retryable_and_never_full_coverage(self):
        with server(lambda path, req: (404, {})) as (url, _):
            with self.assertRaisesRegex(NotReady, '^Gloas FULL execution envelope not observed at this boundary block$'):
                execution_payload(endpoint(url), block_response('gloas'), Header(64, ROOT))

    def test_signed_gloas_bid_supported_without_obsolete_parent_envelope_field(self):
        response = block_response('gloas')
        body = response['data']['message']['body']
        body['signed_execution_payload_bid'] = {'signature': SIGNATURE, 'message': body.pop('execution_payload_bid')}
        with server(lambda path, req: (200, envelope())) as (url, _):
            self.assertEqual(execution_payload(endpoint(url), response, Header(64, ROOT))['slot_number'], '64')

    def test_block_header_and_state_api_root_must_agree(self):
        def route(path, request):
            if '/blocks/' in path:
                return 200, block_response('phase0', 1)
            if '/headers/' in path:
                return 200, {'data': {'canonical': True, 'root': ROOT, 'header': {'message': {'slot': '1', 'state_root': STATE}}}}
            self.assertEqual(path, '/eth/v1/beacon/states/1/root')
            return 200, {'data': {'root': PARENT}, 'execution_optimistic': False, 'finalized': True}
        with server(route) as (url, _):
            with self.assertRaisesRegex(CheckError, '^beacon block/header/state API roots differ$'):
                fixed_beacon(endpoint(url), 1)

    def test_known_state_root_fallback_preserves_history_and_records_lookup(self):
        def route(path, request):
            if path == '/eth/v2/beacon/blocks/1':
                return 200, block_response('phase0', 1)
            if path == '/eth/v1/beacon/headers/1':
                return 200, {'data': {'canonical': True, 'root': ROOT, 'header': {'message': {'slot': '1', 'state_root': STATE}}}}
            if path == '/eth/v1/beacon/states/1/root':
                return 404, {'message': 'historic slot index unavailable'}
            self.assertEqual(path, '/eth/v1/beacon/states/' + STATE + '/root')
            return 200, {'data': {'root': STATE}, 'execution_optimistic': False, 'finalized': True}
        with server(route) as (url, calls):
            result = fixed_beacon(endpoint(url), 1)
            self.assertEqual(result[3], STATE)
            self.assertEqual(result[4], 'root')
            self.assertEqual(len(calls), 4)
            config = {'slots_per_epoch': 32, 'forks': [{'name': 'phase0', 'epoch': 0}]}
            coverage = observe_fork(config['forks'][0], config, 1, endpoint(url), endpoint(url), None, None, {}, True)
            self.assertEqual(coverage['state_lookup'], {'sut': 'root', 'reference': 'root'})
            self.assertFalse(coverage['independent_ssz_root_computation'])

    def test_missing_state_in_both_indexes_cannot_earn_coverage(self):
        def route(path, request):
            if '/blocks/' in path:
                return 200, block_response('phase0', 1)
            if '/headers/' in path:
                return 200, {'data': {'canonical': True, 'root': ROOT, 'header': {'message': {'slot': '1', 'state_root': STATE}}}}
            return 404, {}
        with server(route) as (url, _):
            with self.assertRaisesRegex(NotReady, '^test has no known finalized state for slot 1$'):
                fixed_beacon(endpoint(url), 1)
            config = {'slots_per_epoch': 32, 'forks': [{'name': 'phase0', 'epoch': 0}]}
            self.assertIsNone(observe_fork(config['forks'][0], config, 1, endpoint(url), endpoint(url), None, None, {}, True))

    def test_root_fallback_must_return_matching_finalized_nonoptimistic_state(self):
        for returned_root, finalized in [(PARENT, True), (STATE, False)]:
            def route(path, request):
                if '/blocks/' in path:
                    return 200, block_response('phase0', 1)
                if '/headers/' in path:
                    return 200, {'data': {'canonical': True, 'root': ROOT, 'header': {'message': {'slot': '1', 'state_root': STATE}}}}
                if path == '/eth/v1/beacon/states/1/root':
                    return 404, {}
                return 200, {'data': {'root': returned_root}, 'execution_optimistic': False, 'finalized': finalized}
            with self.subTest(returned_root=returned_root, finalized=finalized), server(route) as (url, _):
                with self.assertRaises(CheckError):
                    fixed_beacon(endpoint(url), 1)
    def test_genesis_fork_does_not_credit_a_block_in_second_epoch(self):
        def route(path, request):
            slot = path.rsplit('/', 1)[1]
            if '/blocks/' in path:
                return (200, block_response('phase0', 32)) if slot == '32' else (404, {})
            if '/headers/' in path:
                return 200, {'data': {'canonical': True, 'root': ROOT, 'header': {'message': {'slot': '32', 'state_root': STATE}}}}
            return 200, {'data': {'root': STATE}, 'execution_optimistic': False, 'finalized': True}
        config = {'slots_per_epoch': 32, 'forks': [{'name': 'phase0', 'epoch': 0}]}
        with server(route) as (url, calls):
            result = observe_fork(config['forks'][0], config, 32, endpoint(url), endpoint(url), None, None, {}, True)
            self.assertIsNone(result)
            self.assertFalse(any(path == '/eth/v2/beacon/blocks/32' for path, _ in calls))
    def test_boundary_wrong_version_rejected_even_when_both_roles_agree(self):
        def route(path, request):
            if '/blocks/' in path:
                return 200, block_response('phase0', 64)
            if '/headers/' in path:
                return 200, {'data': {'canonical': True, 'root': ROOT, 'header': {'message': {'slot': '64', 'state_root': STATE}}}}
            return 200, {'data': {'root': STATE}, 'execution_optimistic': False, 'finalized': True}
        config = {'slots_per_epoch': 32, 'forks': [{'name': 'phase0', 'epoch': 0}, {'name': 'altair', 'epoch': 2}]}
        with server(route) as (url, _):
            with self.assertRaisesRegex(CheckError, '^scheduled fork altair did not activate at its boundary$'):
                observe_fork(config['forks'][1], config, 64, endpoint(url), endpoint(url), None, None, {}, True)


class ExecutionContracts(unittest.TestCase):
    def test_absent_finality_is_retryable_but_unsupported_tag_is_failure(self):
        for code in (-32000, -32001, -32602):
            with self.subTest(code=code), server(lambda path, req: (200, {'jsonrpc': '2.0', 'id': req['id'], 'error': {'code': code, 'message': 'private error details'}})) as (url, _):
                if code in (-32000, -32001):
                    with self.assertRaisesRegex(NotReady, '^test has no finalized execution block yet$'):
                        finalized_el(endpoint(url))
                else:
                    with self.assertRaises(CheckError) as caught:
                        finalized_el(endpoint(url))
                    self.assertNotIsInstance(caught.exception, NotReady)
                    self.assertEqual(str(caught.exception), 'test RPC eth_getBlockByNumber failed (code -32602)')
    def test_comparison_uses_one_finalized_height_despite_different_heads(self):
        def route(path, request):
            if request['method'] == 'eth_getBlockByNumber' and request['params'][0] == 'finalized':
                return 200, {'jsonrpc': '2.0', 'id': request['id'], 'result': execution_block(12 if path == '/sut' else 9)}
            return rpc_route(path, request)
        with server(route) as (url, calls):
            result = compare_execution(endpoint(url + '/sut'), endpoint(url + '/reference'), 7, payload())
            self.assertEqual(result['height'], 7)
            self.assertEqual(result['transaction_count'], 1)
            fixed = [req['params'][0] for _, req in calls if req['method'] == 'eth_getBlockByNumber' and req['params'][0] != 'finalized']
            self.assertEqual(fixed, ['0x7'] * 4)
            self.assertEqual(len(result['receipts'][0]['receipt_digest']), 64)

    def test_same_wrong_receipt_height_on_both_roles_still_fails(self):
        def route(path, request):
            status, response = rpc_route(path, request)
            if request['method'] == 'eth_getTransactionReceipt':
                response['result']['blockNumber'] = '0x8'
            return status, response
        with server(route) as (url, _):
            with self.assertRaisesRegex(CheckError, '^receipt belongs to a different finalized block height$'):
                compare_execution(endpoint(url + '/sut'), endpoint(url + '/reference'), 7)

    def test_requested_height_is_not_silently_replaced_by_latest(self):
        def route(path, request):
            if request['method'] == 'eth_getBlockByNumber':
                return 200, {'jsonrpc': '2.0', 'id': request['id'], 'result': execution_block(9)}
            return rpc_route(path, request)
        with server(route) as (url, _):
            with self.assertRaisesRegex(CheckError, '^execution endpoint returned a different requested block height$'):
                compare_execution(endpoint(url), endpoint(url), 7)

    def test_matching_hash_does_not_mask_state_root_disagreement(self):
        def route(path, request):
            status, response = rpc_route(path, request)
            if path == '/sut' and request['method'] == 'eth_getBlockByNumber' and request['params'][0] == '0x7':
                response['result']['stateRoot'] = PARENT
            return status, response
        with server(route) as (url, _):
            with self.assertRaisesRegex(CheckError, '^finalized blocks differ in stateRoot at height 7$'):
                compare_execution(endpoint(url + '/sut'), endpoint(url + '/reference'), 7)

    def test_fixed_hash_rechecked_after_receipts(self):
        seen = 0
        def route(path, request):
            nonlocal seen
            status, response = rpc_route(path, request)
            if request['method'] == 'eth_getBlockByNumber' and request['params'][0] == '0x7':
                seen += 1
                if seen > 2:
                    response['result']['hash'] = ROOT
            return status, response
        with server(route) as (url, _):
            with self.assertRaisesRegex(CheckError, '^a finalized height changed during receipt collection$'):
                compare_execution(endpoint(url), endpoint(url), 7)


class MergeContracts(unittest.TestCase):
    def test_advertised_terminal_identity_and_epoch_must_match_real_genesis(self):
        for terminal, activation, expected in [(PARENT, 4, 'configured terminal block differs from shared execution genesis'), (ROOT, 3, 'configured terminal block activation differs from planned Bellatrix epoch')]:
            routes = config_routes(list(range(0, 16, 2)))
            routes['/eth/v1/config/spec']['data'].update({'TERMINAL_BLOCK_HASH': terminal, 'TERMINAL_BLOCK_HASH_ACTIVATION_EPOCH': str(activation)})
            with self.subTest(terminal=terminal, activation=activation), server(lambda path, req: (200, routes[path])) as (url, _):
                deployed = schedule(endpoint(url))
                planned = {'expected_forks': list(FORKS), 'merge_transition': {'terminal_block': 'execution_genesis', 'activation_epoch': 4}}
                with self.assertRaisesRegex(CheckError, '^' + expected + '$'):
                    merge_contract(deployed, planned, {'genesis_execution_hash': ROOT}, False)

    def test_undisclosed_terminal_configuration_cannot_credit_merge(self):
        with server(lambda path, req: (200, config_routes()[path])) as (url, _):
            deployed = schedule(endpoint(url))
            planned = {'expected_forks': ['bellatrix'], 'merge_transition': {'terminal_block': 'execution_genesis', 'activation_epoch': 4}}
            with self.assertRaisesRegex(CheckError, '^historical merge requires advertised terminal block configuration$'):
                merge_contract(deployed, planned, {'genesis_execution_hash': ROOT}, False)
            self.assertIsNone(merge_contract(deployed, {'expected_forks': ['gloas']}, {'genesis_execution_hash': ROOT}, False))

    def test_payload_parent_must_match_fixed_execution_block_even_when_roles_agree(self):
        value = payload()
        value['parent_hash'] = ROOT
        with server(rpc_route) as (url, _):
            with self.assertRaisesRegex(CheckError, '^beacon payload parent_hash differs from finalized execution block$'):
                compare_execution(endpoint(url), endpoint(url), 7, value)

    def observe(self, height=1, parent=ROOT, empty=False):
        def route(path, request):
            if request is not None:
                status, response = rpc_route(path, request)
                if request['method'] == 'eth_getBlockByNumber':
                    response['result']['parentHash'] = ROOT
                    response['result']['transactions'] = []
                    response['result']['gasUsed'] = '0x0'
                    for field in ('blobGasUsed', 'excessBlobGas', 'withdrawals'):
                        del response['result'][field]
                return status, response
            if '/blocks/' in path:
                slot = int(path.rsplit('/', 1)[1])
                response = block_response('bellatrix' if slot >= 128 else 'altair', slot)
                if slot >= 128:
                    value = response['data']['message']['body']['execution_payload']
                    value.update({'block_number': str(height), 'parent_hash': parent, 'transactions': [], 'gas_used': '0'})
                    if empty:
                        value['block_hash'] = '0x' + '00' * 32
                return 200, response
            if '/headers/' in path:
                slot = path.rsplit('/', 1)[1]
                return 200, {'data': {'canonical': True, 'root': ROOT, 'header': {'message': {'slot': slot, 'state_root': STATE}}}}
            return 200, {'data': {'root': STATE}, 'execution_optimistic': False, 'finalized': True}
        config = {'slots_per_epoch': 32, 'forks': [{'name': 'phase0', 'epoch': 0}, {'name': 'altair', 'epoch': 2}, {'name': 'bellatrix', 'epoch': 4}]}
        merge = {'terminal_block_hash': ROOT, 'activation_epoch': 4, 'first_accepted_execution': None}
        comparisons = {}
        with server(route) as (url, _):
            evidence = observe_fork(config['forks'][2], config, 128, endpoint(url), endpoint(url), endpoint(url), endpoint(url), comparisons, False, merge)
        return evidence, merge, comparisons

    def test_first_accepted_merge_block_is_finalized_height_one_with_canonical_parent(self):
        evidence, merge, comparisons = self.observe()
        self.assertEqual(evidence['fork'], 'bellatrix')
        self.assertEqual(evidence['before_boundary']['version'], 'altair')
        self.assertEqual(merge['first_accepted_execution'], {'height': 1, 'hash': HASH, 'parent_hash': ROOT, 'beacon_slot': 128, 'beacon_root': ROOT})
        self.assertEqual(comparisons[1]['parent_hash'], ROOT)

    def test_later_execution_block_or_wrong_terminal_parent_cannot_credit_merge(self):
        for height, parent in [(2, ROOT), (1, PARENT)]:
            with self.subTest(height=height, parent=parent), self.assertRaisesRegex(CheckError, '^Bellatrix boundary must contain the first accepted execution block with the canonical terminal parent$'):
                self.observe(height=height, parent=parent)

    def test_bellatrix_version_with_empty_payload_is_not_an_accepted_transition(self):
        evidence, merge, comparisons = self.observe(empty=True)
        self.assertIsNone(evidence)
        self.assertIsNone(merge['first_accepted_execution'])
        self.assertEqual(comparisons, {})


class ReadinessContracts(unittest.TestCase):
    def files(self, directory, probe=None):
        path = Path(directory) / 'readiness.json'
        path.write_text(json.dumps({'target_fork': 'gloas', **dict.fromkeys(READINESS_FIELDS, False)}), encoding='utf-8')
        probe_path = Path(directory) / 'probe.json'
        probe_path.write_text(json.dumps(probe or {'exit_code': 1, 'datadir_initialized': False, 'readiness_error': True}), encoding='utf-8')
        return path, probe_path

    def test_strict_readiness_blocks_before_any_network_request(self):
        with tempfile.TemporaryDirectory(dir=CHECKS) as directory, server(lambda path, req: (500, {})) as (url, calls):
            readiness, probe = self.files(directory)
            args = ['--sut-el-url', url, '--reference-el-url', url, '--sut-beacon-url', url, '--reference-beacon-url', url, '--expected-forks', 'gloas', '--readiness-file', str(readiness), '--embedded-launch-probe', str(probe), '--require-embedded-ready', '--output-dir', directory]
            with redirect_stdout(io.StringIO()):
                self.assertEqual(main(args), 2)
            report = json.loads((Path(directory) / 'verification.json').read_text())
            self.assertEqual(calls, [])
            self.assertEqual(report['status'], 'blocked')
            self.assertFalse(report['embedded_runtime_acceptance'])
            self.assertTrue(report['embedded_readiness']['fail_closed_probe_verified'])

    def test_expected_coverage_can_be_derived_from_schedule(self):
        with tempfile.TemporaryDirectory(dir=CHECKS) as directory:
            readiness, probe = self.files(directory)
            profile = CHECKS.parent / 'package/schedules/gloas-transition.json'
            args = ['--sut-el-url', 'http://127.0.0.1:1', '--reference-el-url', 'http://127.0.0.1:1', '--sut-beacon-url', 'http://127.0.0.1:1', '--reference-beacon-url', 'http://127.0.0.1:1', '--schedule', str(profile), '--readiness-file', str(readiness), '--embedded-launch-probe', str(probe), '--require-embedded-ready', '--output-dir', directory]
            with redirect_stdout(io.StringIO()):
                self.assertEqual(main(args), 2)
            report = json.loads((Path(directory) / 'verification.json').read_text())
            self.assertEqual(report['required_forks'], ['fulu', 'gloas'])

    def test_false_readiness_cannot_accept_successful_or_mutating_launch(self):
        for probe in [{'exit_code': 0, 'datadir_initialized': False, 'readiness_error': True}, {'exit_code': 1, 'datadir_initialized': True, 'readiness_error': True}]:
            with self.subTest(probe=probe), tempfile.TemporaryDirectory(dir=CHECKS) as directory:
                readiness, path = self.files(directory, probe)
                with self.assertRaisesRegex(CheckError, '^incomplete embedded runtime failed to stop before database initialization$'):
                    readiness_gate(readiness, path)

    def test_unrelated_cli_failure_cannot_qualify_as_readiness_protection(self):
        with tempfile.TemporaryDirectory(dir=CHECKS) as directory:
            readiness, path = self.files(directory, {'exit_code': 1, 'datadir_initialized': False, 'readiness_error': False})
            with self.assertRaisesRegex(CheckError, '^embedded launch probe failed for a reason other than readiness$'):
                readiness_gate(readiness, path)
    def test_readiness_requires_boolean_capabilities(self):
        with tempfile.TemporaryDirectory(dir=CHECKS) as directory:
            readiness, _ = self.files(directory)
            value = json.loads(readiness.read_text())
            value['peer_runtime'] = 'false'
            readiness.write_text(json.dumps(value))
            with self.assertRaisesRegex(CheckError, '^embedded readiness artifact lacks required Gloas capabilities$'):
                readiness_gate(readiness)


class ProvenanceContracts(unittest.TestCase):
    def check(self, mutate=None, docker_failure=False):
        urls = {'reference_execution': 'http://127.0.0.1:16001', 'reference_beacon': 'http://127.0.0.1:16002', 'sut_execution': 'http://127.0.0.1:16003', 'sut_beacon': 'http://127.0.0.1:16004'}
        expected, containers, inspect = independent_fixture(urls)
        args = SimpleNamespace(reference_el_url=urls['reference_execution'], reference_beacon_url=urls['reference_beacon'], sut_el_url=urls['sut_execution'], sut_beacon_url=urls['sut_beacon'])
        if mutate:
            mutate(expected, containers, args)
        with tempfile.TemporaryDirectory(dir=CHECKS) as directory:
            path = Path(directory) / 'provenance.json'
            path.write_text(json.dumps(expected))
            side_effect = CheckError('live Docker deployment inspection is unavailable') if docker_failure else inspect
            with patch('provenance.docker_output', side_effect=side_effect):
                return deployment_provenance(path, args)

    def test_actual_inspection_binds_both_apis_to_one_sut_with_expected_image(self):
        result = self.check()
        self.assertEqual(result['source'], 'live_docker_inspection')
        self.assertEqual(result['roles']['sut']['container_id'], '3' * 64)
        self.assertEqual(result['endpoint_bindings']['sut_execution']['container_port'], '8545/tcp')
        self.assertEqual(result['endpoint_bindings']['sut_beacon']['container_port'], '5052/tcp')

    def test_asserted_provenance_cannot_replace_live_inspection(self):
        with self.assertRaisesRegex(CheckError, '^live Docker deployment inspection is unavailable$'):
            self.check(docker_failure=True)

    def test_missing_embedded_flag_or_substituted_external_consensus_is_rejected(self):
        with self.assertRaisesRegex(CheckError, '^live SUT was not launched with the embedded beacon flag$'):
            self.check(lambda plan, containers, args: containers[-1]['Config'].update({'Entrypoint': ['/external-beacon'], 'Cmd': ['beacon']}))

    def test_sut_consensus_url_cannot_point_to_reference_container_or_remote_proxy(self):
        for url in ['http://127.0.0.1:16002', 'http://192.0.2.1:16004', 'http://localhost:16004', 'http://127.0.0.1:16004/proxy', 'http://127.0.0.1:16004?token=secret']:
            with self.subTest(url=url), self.assertRaises(CheckError):
                self.check(lambda plan, containers, args: setattr(args, 'sut_beacon_url', url))

    def test_wrong_role_alias_wrong_image_and_stopped_sut_fail(self):
        mutations = [lambda plan, containers, args: plan['roles'].update({'sut': 'ref-beacon'}), lambda plan, containers, args: containers[-1].update({'Image': 'sha256:' + 'f' * 64}), lambda plan, containers, args: containers[-1]['State'].update({'Running': False})]
        for mutate in mutations:
            with self.subTest(mutate=mutate), self.assertRaises(CheckError):
                self.check(mutate)

    def test_extra_unplanned_sut_consensus_container_and_label_spoof_fail(self):
        def extra(plan, containers, args):
            duplicate = deepcopy(containers[0])
            duplicate['Id'] = '4' * 64
            duplicate['Config']['Labels'].update({'com.kurtosistech.id': 'cl-2-extra', 'kurtosis_service_name': 'cl-2-extra'})
            containers.append(duplicate)
        for mutate in [extra, lambda plan, containers, args: containers[-1]['Config']['Labels'].update({'kurtosis_service_name': 'ref-beacon'})]:
            with self.subTest(mutate=mutate), self.assertRaises(CheckError):
                self.check(mutate)

    def test_malformed_command_and_missing_port_fail_with_contract_error(self):
        for mutate in [lambda plan, containers, args: containers[-1]['Config'].update({'Cmd': 'node --embedded-beacon'}), lambda plan, containers, args: containers[-1]['NetworkSettings'].update({'Ports': None})]:
            with self.subTest(mutate=mutate), self.assertRaises(CheckError):
                self.check(mutate)


class IndependentPreflight(unittest.TestCase):
    def invoke(self, ready, preflight=True, mode='independent_embedded'):
        with tempfile.TemporaryDirectory(dir=CHECKS) as directory:
            status = Path(directory) / 'readiness.json'
            status.write_text(json.dumps({'target_fork': 'gloas', **dict.fromkeys(READINESS_FIELDS, ready)}))
            plan = Path(directory) / 'schedule.json'
            plan.write_text(json.dumps({'preset': 'mainnet', 'slots_per_epoch': 32, 'slot_duration_ms': 6000, 'consensus_mode': mode, 'expected_forks': ['gloas']}))
            args = ['--schedule', str(plan), '--readiness-file', str(status), '--output-dir', directory]
            if preflight:
                args.append('--preflight-only')
            with redirect_stdout(io.StringIO()), patch('provenance.docker_output') as docker:
                code = main(args)
            self.assertEqual(docker.call_count, 0)
            return code, json.loads((Path(directory) / 'verification.json').read_text())

    def test_false_runtime_blocks_by_default_without_urls_or_opt_in_strict_flag(self):
        for preflight in [False, True]:
            code, report = self.invoke(False, preflight)
            self.assertEqual(code, 2)
            self.assertEqual(report['status'], 'blocked')
            self.assertFalse(report['embedded_runtime_acceptance'])
            self.assertNotIn('baseline_passed', report)

    def test_ready_preflight_reports_not_deployed_without_crediting_acceptance(self):
        code, report = self.invoke(True)
        self.assertEqual(code, 0)
        self.assertEqual(report['status'], 'ready_not_deployed')
        self.assertFalse(report['embedded_runtime_acceptance'])
        self.assertFalse(report['public_network_sync_acceptance'])

    def test_ready_external_mode_and_missing_endpoints_cannot_receive_acceptance(self):
        for preflight, mode in [(True, 'external_baseline'), (False, 'independent_embedded')]:
            code, report = self.invoke(True, preflight, mode)
            self.assertEqual(code, 1)
            self.assertEqual(report['status'], 'failed')
            self.assertFalse(report['embedded_runtime_acceptance'])


class BaselineFlow(unittest.TestCase):
    def test_real_network_contracts_require_live_independent_provenance_before_acceptance(self):
        routes = config_routes([0, 0, 0] + [FAR_FUTURE_EPOCH] * 5)
        routes['/eth/v1/config/spec']['data']['SLOT_DURATION_MS'] = '6000'
        heads = {'sut-beacon': 0, 'reference-beacon': 0}

        def number_hash(value):
            return '0x' + value.to_bytes(32, 'big').hex()

        def block(height):
            value = execution_block(height, version='bellatrix')
            value['hash'] = number_hash(height + 20)
            value['transactions'] = [] if height == 0 else [number_hash(height)]
            return value

        def route(path, request):
            role, _, api = path[1:].partition('/')
            api = '/' + api
            if request is not None:
                method, params = request['method'], request['params']
                if method == 'eth_chainId':
                    result = '0x1'
                elif method == 'eth_getBlockByNumber':
                    height = 9 if min(heads.values()) <= 1 else 10
                    result = block(height if params[0] == 'finalized' else int(params[0], 16))
                elif method == 'eth_getRawTransactionByHash':
                    result = '0x01'
                else:
                    height = int(params[0], 16)
                    result = receipt(height)
                    result['transactionHash'] = number_hash(height)
                    result['blockHash'] = number_hash(height + 20)
                return 200, {'jsonrpc': '2.0', 'id': request['id'], 'result': result}
            if api in routes:
                return 200, routes[api]
            if '/headers/' in api:
                requested = api.rsplit('/', 1)[1]
                if requested == 'finalized':
                    heads[role] += 1
                    slot = min(heads[role], 2)
                else:
                    slot = int(requested)
                return 200, {'execution_optimistic': False, 'data': {'canonical': True, 'root': number_hash(slot + 100), 'header': {'message': {'slot': str(slot), 'state_root': STATE}}}}
            if '/blocks/' in api:
                slot = int(api.rsplit('/', 1)[1])
                result = block_response('bellatrix', slot)
                value = result['data']['message']['body']['execution_payload']
                value['block_number'] = str(slot + 6)
                value['block_hash'] = number_hash(slot + 26)
                return 200, result
            if '/states/' in api:
                return 200, {'data': {'root': STATE}, 'execution_optimistic': False, 'finalized': True}
            return 404, {}

        with ExitStack() as stack:
            directory = stack.enter_context(tempfile.TemporaryDirectory(dir=CHECKS))
            urls, call_lists = {}, []
            for key, role in [('sut_execution', 'sut-execution'), ('reference_execution', 'reference-execution'), ('sut_beacon', 'sut-beacon'), ('reference_beacon', 'reference-beacon')]:
                url, requests = stack.enter_context(server(lambda path, request, role=role: route('/' + role + path, request)))
                urls[key] = url
                call_lists.append(requests)
            expected, _, inspect = independent_fixture(urls)
            provenance_path = Path(directory) / 'provenance.json'
            provenance_path.write_text(json.dumps(expected))
            plan = {'preset': 'mainnet', 'consensus_mode': 'independent_embedded', 'genesis_fork': 'bellatrix', 'slots_per_epoch': 32, 'slot_duration_ms': 6000, 'expected_forks': ['bellatrix'], 'forks': [{'name': name, 'epoch': int(entry['epoch']), 'version': entry['current_version']} for name, entry in zip(FORKS, routes['/eth/v1/config/fork_schedule']['data'])]}
            plan_path = Path(directory) / 'plan.json'
            plan_path.write_text(json.dumps(plan))
            readiness = Path(directory) / 'readiness.json'
            readiness.write_text(json.dumps({'target_fork': 'gloas', **dict.fromkeys(READINESS_FIELDS, True)}), encoding='utf-8')
            args = ['--sut-el-url', urls['sut_execution'], '--reference-el-url', urls['reference_execution'], '--sut-beacon-url', urls['sut_beacon'], '--reference-beacon-url', urls['reference_beacon'], '--schedule', str(plan_path), '--readiness-file', str(readiness), '--independent-runtime-provenance', str(provenance_path), '--output-dir', directory, '--timeout-seconds', '2', '--poll-interval', '0.001', '--min-finalized-blocks', '1']
            with redirect_stdout(io.StringIO()), patch('provenance.docker_output', side_effect=inspect):
                self.assertEqual(main(args), 0)
            calls = [entry for items in call_lists for entry in items]
            report = json.loads((Path(directory) / 'verification.json').read_text())
            self.assertEqual(report['status'], 'passed')
            self.assertEqual(report['test_topology'], 'reference_pair_and_embedded_node')
            self.assertEqual(report['deployment_provenance']['source'], 'live_docker_inspection')
            self.assertNotIn('waiting_for', report)
            self.assertTrue(report['embedded_runtime_acceptance'])
            self.assertTrue(report['embedded_readiness']['ready'])
            self.assertFalse(report['public_network_sync_acceptance'])
            self.assertEqual(report['fork_coverage'][0]['fork'], 'bellatrix')
            self.assertGreaterEqual(report['finalized_transaction_receipts_checked'], 1)
            self.assertEqual(report['initial_finality']['common_execution_height'], 9)
            self.assertGreater(report['latest_finality']['sut_beacon_slot'], report['initial_finality']['sut_beacon_slot'])
            tags = [request['params'][0] for _, request in calls if request and request['method'] == 'eth_getBlockByNumber']
            self.assertNotIn('latest', tags)

if __name__ == '__main__':
    unittest.main()
