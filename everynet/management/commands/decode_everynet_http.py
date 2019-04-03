import base64
import binascii
import datetime
import logging

import pytz
from django.conf import settings

from broker.management.commands import RabbitCommand
from broker.utils import (
    create_dataline, create_parsed_data_message,
    data_pack, data_unpack,
    decode_json_body, get_datalogger, decode_payload,
    create_routing_key, send_message
)

logger = logging.getLogger('django')


def parse_everynet_request(serialised_request, body):
    # FIXME: currently this parses only payload_hex from PAXCOUNTER. MUST check BKS too and others
    # TODO: create utility function, which extracts all interesting fields (needed here) out from request data
    devid = body['meta'].get('device', 'unknown')
    payload_base64 = body['params']['payload']
    payload_hex = binascii.hexlify(base64.b64decode(payload_base64))
    port = body['params']['port']
    rssi = body['params']['radio']['hardware']['rssi']
    timestamp = datetime.datetime.utcfromtimestamp(body['params']['rx_time'])
    timestamp = timestamp.astimezone(pytz.UTC)
    # TODO: this may fail if database is offline
    datalogger, created = get_datalogger(devid=devid, update_activity=False)
    payload = decode_payload(datalogger, payload_hex, port)
    payload['rssi'] = rssi

    # RabbitMQ part
    key = create_routing_key('everynet', devid)
    dataline = create_dataline(timestamp, payload)
    datalines = [dataline]
    message = create_parsed_data_message(devid, datalines=datalines)
    packed_message = data_pack(message)
    send_message(settings.PARSED_DATA_EXCHANGE, key, packed_message)
    return True


def consumer_callback(channel, method, properties, body, options=None):
    serialised_request = data_unpack(body)
    ok, body = decode_json_body(serialised_request['request.body'])
    if body.get('type') == 'uplink':
        parse_everynet_request(serialised_request, body)
    # TODO: create function to normalise everynet json
    channel.basic_ack(method.delivery_tag)


class Command(RabbitCommand):
    help = 'Decode everynet'

    def add_arguments(self, parser):
        parser.add_argument('--prefix', type=str,
                            help='queue and routing_key prefix, overrides settings.ROUTING_KEY_PREFIX')
        super().add_arguments(parser)

    def handle(self, *args, **options):
        logger.info(f'Start handling {__name__}')
        name = 'everynet'
        # FIXME: constructing options should be in a function in broker.utils
        if options["prefix"] is None:
            prefix = settings.RABBITMQ["ROUTING_KEY_PREFIX"]
        else:
            prefix = options["prefix"]
        options['exchange'] = settings.RAW_HTTP_EXCHANGE
        options['routing_key'] = f'{prefix}.{name}.#'
        options['queue'] = f'{prefix}_decode_{name}_http_queue'
        options['consumer_callback'] = consumer_callback
        super().handle(*args, **options)
