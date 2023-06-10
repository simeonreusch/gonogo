import datetime
import json
import logging
from base64 import b64decode
from io import BytesIO
from pprint import pprint
from typing import Dict

import astropy_healpix as ah  # type: ignore
import numpy as np
from astropy.table import Table  # type: ignore
from confluent_kafka import TopicPartition  # type: ignore
from gcn_kafka import Consumer  # type: ignore

PNS_THRESHOLD_DELIBERATE: float = 0.1
PNS_THRESHOLD_GO: float = 0.5
FAR_THRESHOLD_YEAR: float = 3.17e-8
FAR_THRESHOLD_DECADE: float = 3.17e-9
FAR_THRESHOLD_CENTURY: float = 3.17e-10
HAS_NS_THRESHOLD_GO: float = 0.9
HAS_NS_THRESHOLD_DELIBERATE: float = 0.1
HAS_REMNANT_THRESHOLD: float = 0.0

logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def parse_notice(record_str: str) -> dict | None:
    """
    Parse the json of the notice and extract location and distance
    """
    record = json.loads(record_str)

    if record["superevent_id"][0] not in ["S"]:
        return None

    if record["alert_type"] == "RETRACTION":
        print(record["superevent_id"], "was retracted")
        return None

    # Respond only to 'CBC' events. Change 'CBC' to 'Burst' to respond to
    # only unmodeled burst events.
    if record["event"]["group"] not in ["CBC", "Burst"]:
        return None

    # Parse sky map
    skymap_str = record.get("event", {}).pop("skymap")
    if skymap_str:
        # Decode, parse skymap, and print most probable sky location
        skymap_bytes = b64decode(skymap_str)
        skymap = Table.read(BytesIO(skymap_bytes))

        level, ipix = ah.uniq_to_level_ipix(
            skymap[np.argmax(skymap["PROBDENSITY"])]["UNIQ"]
        )
        ra, dec = ah.healpix_to_lonlat(ipix, ah.level_to_nside(level), order="nested")

        dist = skymap.meta["DISTMEAN"]
        dist_unc = skymap.meta["DISTSTD"]

        return {"loc": [ra, dec], "dist": [dist, dist_unc], "record": record}

    else:
        return None


def decide(record: dict) -> dict | None:
    """
    Evaluate the alert content, decide if it warrants further scrutiny or if it is an automatic nogo
    """
    status: Dict[str, str | list] = {"status": "undecided"}
    reason: list = []
    details = record.get("record", {})

    if details is not None:
        event_id = details["superevent_id"]
        event = details["event"]
        far = event["far"]
        classification = event["classification"]
        has_ns = event["properties"]["HasNS"]
        has_remnant = event["properties"]["HasRemnant"]
        bns = classification["BNS"]
        combined_ns = classification["BNS"] + classification["NSBH"]

        status["id"] = event_id
        status["FAR"] = far
        status["pNS"] = combined_ns
        status["hasRemnant"] = has_remnant

        if (
            combined_ns > PNS_THRESHOLD_GO
            and has_ns > HAS_NS_THRESHOLD_GO
            and has_remnant > HAS_REMNANT_THRESHOLD
        ):
            if far < FAR_THRESHOLD_CENTURY:
                status["status"] = "go_deep"
                reason.append(f"FAR < 1/century ({far})")
                status["reason"] = reason

                return status

            if far < FAR_THRESHOLD_DECADE:
                status["status"] = "go_deep"
                reason.append(f"1/century < FAR < 1/decade ({far})")
                status["reason"] = reason

                return status

        if (
            far < FAR_THRESHOLD_YEAR
            and combined_ns > PNS_THRESHOLD_DELIBERATE
            and has_ns > HAS_NS_THRESHOLD_DELIBERATE
        ):
            status["status"] = "deliberate"
            reason.append(f"FAR < 1/year ({far})")
            status["reason"] = reason

            return status

        else:
            status["status"] = "nogo"
            if far >= FAR_THRESHOLD_YEAR:
                reason.append(f"FAR >= 1/year ({far})")
            if has_ns <= HAS_NS_THRESHOLD_DELIBERATE:
                reason.append(f"hasNS <= {HAS_NS_THRESHOLD_DELIBERATE} ({has_ns})")
            if combined_ns <= PNS_THRESHOLD_DELIBERATE:
                reason.append(f"pNS <=  {PNS_THRESHOLD_DELIBERATE} ({combined_ns})")
            status["reason"] = reason

            return status

        return status

    else:
        return None


consumer = Consumer(
    client_id="2c954gdsc6l3cv8p6al5prv08s",
    client_secret="1c7n24c4pptu57s5je3os3m32vedrksgttvq677cdn1pmfkhjv3g",
)
consumer.subscribe(["igwn.gwalert"])

logger.info("Listening to Kafka stream")

while True:
    for message in consumer.consume():
        record = parse_notice(message.value())
        if record is not None:
            decision = decide(record)
            logger.info(decision)

# with open("MS181101ab-preliminary.json", "r") as f:
#     infile = f.read()

# record = parse_notice(infile)
# decision = decide(record)
# print(decision)
