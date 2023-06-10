import datetime
import json
from base64 import b64decode
from io import BytesIO
from pprint import pprint

import astropy_healpix as ah
import numpy as np
from astropy.table import Table
from confluent_kafka import TopicPartition
from gcn_kafka import Consumer

PNS_THRESHOLD_DELIBERATE = 0.1
PNS_THRESHOLD_GO = 0.5
FAR_THRESHOLD_YEAR = 3.17e-8
FAR_THRESHOLD_DECADE = 3.17e-9
FAR_THRESHOLD_CENTURY = 3.17e-10
HAS_NS_THRESHOLD_GO = 0.9
HAS_NS_THRESHOLD_DELIBERATE = 0.1
HAS_REMNANT_THRESHOLD = 0


def parse_notice(record: str) -> dict | None:
    """
    Parse the json of the notice and extract location and distance
    """
    record = json.loads(record)

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


def decide(record: dict) -> dict:
    """
    Evaluate the alert content, decide if it warrants further scrutiny or if it is an automatic nogo
    """
    status = {"status": "undecided", "reason": []}
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
                reason = status["reason"]
                reason.append(f"FAR < 1/century ({far})")
                status["reason"] = reason

                return status

            if far < FAR_THRESHOLD_DECADE:
                status["status"] = "go_deep"
                reason = status["reason"]
                reason.append(f"1/century < FAR < 1/decade ({far})")
                status["reason"] = reason

                return status

        if (
            far < FAR_THRESHOLD_YEAR
            and combined_ns > PNS_THRESHOLD_DELIBERATE
            and has_ns > HAS_NS_THRESHOLD_DELIBERATE
        ):
            status["status"] = "deliberate"
            reason = status["reason"]
            reason.append(f"FAR < 1/year ({far})")
            status["reason"] = reason

            return status

        else:
            status["status"] = "nogo"
            reason = status["reason"]
            if far >= FAR_THRESHOLD_YEAR:
                reason.append(f"FAR >= 1/year ({far})")
            if has_ns <= HAS_NS_THRESHOLD_DELIBERATE:
                reason.append(f"hasNS <= {HAS_NS_THRESHOLD_DELIBERATE} ({has_ns})")
            if combined_ns <= PNS_THRESHOLD_DELIBERATE:
                reason.append(f"pNS <=  {PNS_THRESHOLD_DELIBERATE} ({combined_ns})")
            status["reason"] = reason

            return status

        return status


consumer = Consumer(
    client_id="2c954gdsc6l3cv8p6al5prv08s",
    client_secret="1c7n24c4pptu57s5je3os3m32vedrksgttvq677cdn1pmfkhjv3g",
)
consumer.subscribe(["igwn.gwalert"])

while True:
    for message in consumer.consume():
        record = parse_notice(message.value())
        decision = decide(record)

# with open("MS181101ab-preliminary.json", "r") as f:
#     infile = f.read()

# record = parse_notice(infile)
# decision = decide(record)
# print(decision)
