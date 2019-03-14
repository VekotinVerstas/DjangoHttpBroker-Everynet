from broker.providers.decoder import DecoderProvider
from everynet.parsers.paxcounter import parse_paxcounter


class PaxcounterDecoder(DecoderProvider):
    description = 'Decode PAXCOUNTER payload'

    def decode_payload(self, hex_payload, port):
        return parse_paxcounter(hex_payload, port)
