#!/usr/bin/env python3
# Author: Simeon Reusch (simeon.reusch@desy.de)
# License: BSD-3-Clause

import datetime
import json
import logging
import os
from base64 import b64decode
from io import BytesIO
from pprint import pprint
from typing import Dict

import numpy as np
from astropy.table import Table  # type: ignore

import astropy_healpix as ah  # type: ignore
from confluent_kafka import TopicPartition  # type: ignore
from gcn_kafka import Consumer  # type: ignore
from slack import WebClient

LVK_GCN_ID = os.environ.get("LVK_GCN_ID")
LVK_GCN_TOKEN = os.environ.get("LVK_GCN_TOKEN")
SLACK_TOKEN = os.environ.get("SLACK_TOKEN_GONOGO")

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
                reason.append(f"FAR < 1/century ({far:.2E})")
                status["reason"] = reason

                return status

            if far < FAR_THRESHOLD_DECADE:
                status["status"] = "go_wide"
                reason.append(f"1/century < FAR < 1/decade ({far:.2E})")
                status["reason"] = reason

                return status

        if (
            far < FAR_THRESHOLD_YEAR
            and combined_ns > PNS_THRESHOLD_DELIBERATE
            and has_ns > HAS_NS_THRESHOLD_DELIBERATE
        ):
            status["status"] = "deliberate"
            reason.append(f"FAR < 1/year ({far:.2E})")
            status["reason"] = reason

            return status

        else:
            status["status"] = "nogo"
            if far >= FAR_THRESHOLD_YEAR:
                reason.append(f"FAR >= 1/year ({far:.2E})")
            if has_ns <= HAS_NS_THRESHOLD_DELIBERATE:
                reason.append(f"hasNS <= {HAS_NS_THRESHOLD_DELIBERATE} ({has_ns:.2f})")
            if combined_ns <= PNS_THRESHOLD_DELIBERATE:
                reason.append(f"pNS <=  {PNS_THRESHOLD_DELIBERATE} ({combined_ns:.2f})")
            status["reason"] = reason

            return status

        return status

    else:
        return None


def post_on_slack(decision: str, slack_client: WebClient) -> None:
    """
    Post the decision on Slack
    """
    if decision["status"] == "nogo":
        text = f"*{decision['id']}: NO GO*\n"
        reason = decision["reason"]
        if len(reason) > 0:
            text += "Reason: "
            for r in reason:
                text += f"{r}   "
    if decision["status"] == "deliberate":
        text = f"*{decision['id']}: DELIBERATE*\nDoes not warrant an automatic Go, but it needs to be discussed if ToO or serendipitous coverage is the right strategy (based on localization and parameters).\n"
        text += f"Parameters: \nFAR: {decision['FAR']:.2E}\np(NS): {decision['pNS']:.2f}\nHas Remnant: {decision['hasRemnant']:.2f}"

    if decision["status"] == "go_wide":
        text = f"*{decision['id']}: GO WIDE*\n"
        reason = decision["reason"]
        if len(reason) > 0:
            text += "Reason: "
            for r in reason:
                text += f"{r}   "

    if decision["status"] == "go_deep":
        text = f"*{decision['id']}: GO DEEP*\n"
        reason = decision["reason"]
        if len(reason) > 0:
            text += "Reason: "
            for r in reason:
                text += f"{r}   "

    slack_client.chat_postMessage(channel="#go-nogo", text=text)


def check_credentials():
    if LVK_GCN_TOKEN is None or LVK_GCN_ID is None or SLACK_TOKEN is None:
        raise ValueError(f"You need to export 'LVK_GCN_ID' and 'LVK_GCN_TOKEN'")


if __name__ == "__main__":
    slack_client = WebClient(token=SLACK_TOKEN)

    check_credentials()

    consumer = Consumer(
        client_id=LVK_GCN_ID,
        client_secret=LVK_GCN_TOKEN,
    )
    consumer.subscribe(["igwn.gwalert"])

    logger.info("Listening to Kafka stream")

    while True:
        for message in consumer.consume():
            record = parse_notice(message.value())
            if record is not None:
                decision = decide(record)
                post_on_slack(decision=decision, slack_client=slack_client)
