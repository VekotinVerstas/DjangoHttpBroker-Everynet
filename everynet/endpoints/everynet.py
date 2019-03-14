import datetime
import json
import logging

from django.conf import settings
from django.http.response import HttpResponse

from broker.providers.endpoint import EndpointProvider
from broker.utils import (
    decode_json_body, get_datalogger, create_routing_key,
    serialize_django_request, data_pack, send_message
)

logger = logging.getLogger('django')


class EverynetEndpoint(EndpointProvider):
    description = "Handle HTTP POST requests from Everynet's LoRaWAN system"

    def handle_request(self, request):
        """
        Decode json body and send serialised request to RabbitMQ exchange
        :param request: Django HttpRequest
        :return: HttpResponse
        """
        if request.method != 'POST':
            return HttpResponse('Only POST with JSON body is allowed', status=405)
        serialised_request = serialize_django_request(request)
        ok, body = decode_json_body(serialised_request['request.body'])
        if ok is False:
            err_msg = f'JSON ERROR: {body}'
            logging.error(err_msg)  # Error or warning?
            return HttpResponse(err_msg, status=400, content_type='text/plain')
        # TODO: this will fail if json is malformed
        devid = body['meta'].get('device', 'unknown')
        serialised_request['devid'] = devid
        serialised_request['time'] = datetime.datetime.utcnow().isoformat() + 'Z'
        logging.debug(json.dumps(body))
        message = data_pack(serialised_request)
        key = create_routing_key('everynet', devid)
        send_message(settings.RAW_HTTP_EXCHANGE, key, message)
        if body.get('type') == 'uplink':
            datalogger, created = get_datalogger(devid=devid, update_activity=True)
            if created:
                logging.info(f'Created new Datalogger {devid}')
        return HttpResponse('OK', content_type='text/plain')
